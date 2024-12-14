import logging
import queue
import time
import uuid
from collections.abc import Generator, Mapping
from concurrent.futures import ThreadPoolExecutor, wait
from copy import copy, deepcopy
from typing import Any, Optional

from flask import Flask, current_app

from core.app.apps.base_app_queue_manager import GenerateTaskStoppedError
from core.app.entities.app_invoke_entities import InvokeFrom
from core.workflow.entities.node_entities import NodeRunMetadataKey
from core.workflow.entities.variable_pool import VariablePool, VariableValue
from core.workflow.graph_engine.condition_handlers.condition_manager import ConditionManager
from core.workflow.graph_engine.entities.event import (
    BaseIterationEvent,
    GraphEngineEvent,
    GraphRunFailedEvent,
    GraphRunStartedEvent,
    GraphRunSucceededEvent,
    NodeRunFailedEvent,
    NodeRunRetrieverResourceEvent,
    NodeRunStartedEvent,
    NodeRunStreamChunkEvent,
    NodeRunSucceededEvent,
    ParallelBranchRunFailedEvent,
    ParallelBranchRunStartedEvent,
    ParallelBranchRunSucceededEvent,
)
from core.workflow.graph_engine.entities.graph import Graph, GraphEdge
from core.workflow.graph_engine.entities.graph_init_params import GraphInitParams
from core.workflow.graph_engine.entities.graph_runtime_state import GraphRuntimeState
from core.workflow.graph_engine.entities.runtime_route_state import RouteNodeState
from core.workflow.nodes import NodeType
from core.workflow.nodes.answer.answer_stream_processor import AnswerStreamProcessor
from core.workflow.nodes.base import BaseNode
from core.workflow.nodes.end.end_stream_processor import EndStreamProcessor
from core.workflow.nodes.event import RunCompletedEvent, RunRetrieverResourceEvent, RunStreamChunkEvent
from core.workflow.nodes.node_mapping import node_type_classes_mapping
from extensions.ext_database import db
from models.enums import UserFrom
from models.workflow import WorkflowNodeExecutionStatus, WorkflowType

logger = logging.getLogger(__name__)


class GraphEngineThreadPool(ThreadPoolExecutor):
    def __init__(
        self, max_workers=None, thread_name_prefix="", initializer=None, initargs=(), max_submit_count=100
    ) -> None:
        super().__init__(max_workers, thread_name_prefix, initializer, initargs)
        self.max_submit_count = max_submit_count
        self.submit_count = 0

    def submit(self, fn, *args, **kwargs):
        self.submit_count += 1
        self.check_is_full()

        return super().submit(fn, *args, **kwargs)

    def task_done_callback(self, future):
        self.submit_count -= 1

    def check_is_full(self) -> None:
        print(f"submit_count: {self.submit_count}, max_submit_count: {self.max_submit_count}")
        if self.submit_count > self.max_submit_count:
            raise ValueError(f"Max submit count {self.max_submit_count} of workflow thread pool reached.")


class GraphEngine:
    # 存储工作流线程池的映射，键为线程池ID，值为对应的线程池实例
    workflow_thread_pool_mapping: dict[str, GraphEngineThreadPool] = {}

    def __init__(
        self,
        tenant_id: str,
        app_id: str,
        workflow_type: WorkflowType,
        workflow_id: str,
        user_id: str,
        user_from: UserFrom,
        invoke_from: InvokeFrom,
        call_depth: int,
        graph: Graph,
        graph_config: Mapping[str, Any],
        variable_pool: VariablePool,
        max_execution_steps: int,
        max_execution_time: int,
        thread_pool_id: Optional[str] = None,
    ) -> None:
        """
        初始化图引擎实例，设置工作流的基本参数和线程池。

        :param tenant_id: 租户ID，标识不同的用户或组织
        :param app_id: 应用ID，标识当前应用
        :param workflow_type: 工作流类型，定义工作流的行为和特性
        :param workflow_id: 工作流ID，唯一标识一个工作流
        :param user_id: 用户ID，标识当前操作的用户
        :param user_from: 用户来源，指示用户是通过何种方式访问应用
        :param invoke_from: 调用来源，指示工作流是如何被触发的
        :param call_depth: 调用深度，记录当前调用的层级
        :param graph: 图结构，表示工作流的节点和边的关系
        :param graph_config: 图的配置，包含图的详细设置
        :param variable_pool: 变量池，存储工作流执行过程中使用的变量
        :param max_execution_steps: 最大执行步骤数，限制工作流的执行步骤
        :param max_execution_time: 最大执行时间，限制工作流的执行时间
        :param thread_pool_id: 线程池ID（可选），用于标识特定的线程池
        """
        # 初始化线程池的最大提交数量和最大工作线程数
        thread_pool_max_submit_count = 100
        thread_pool_max_workers = 10

        # 根据提供的线程池ID初始化线程池
        if thread_pool_id:
            if thread_pool_id not in GraphEngine.workflow_thread_pool_mapping:
                raise ValueError(f"Max submit count {thread_pool_max_submit_count} of workflow thread pool reached.")

            self.thread_pool_id = thread_pool_id
            self.thread_pool = GraphEngine.workflow_thread_pool_mapping[thread_pool_id]
            self.is_main_thread_pool = False
        else:
            # 创建新的线程池实例
            self.thread_pool = GraphEngineThreadPool(
                max_workers=thread_pool_max_workers, max_submit_count=thread_pool_max_submit_count
            )
            self.thread_pool_id = str(uuid.uuid4())  # 生成唯一的线程池ID
            self.is_main_thread_pool = True  # 标记为主线程池
            GraphEngine.workflow_thread_pool_mapping[self.thread_pool_id] = self.thread_pool  # 添加到映射中

        # 设置图和初始化参数
        self.graph = graph
        self.init_params = GraphInitParams(
            tenant_id=tenant_id,
            app_id=app_id,
            workflow_type=workflow_type,
            workflow_id=workflow_id,
            graph_config=graph_config,
            user_id=user_id,
            user_from=user_from,
            invoke_from=invoke_from,
            call_depth=call_depth,
        )

        # 初始化图运行状态
        self.graph_runtime_state = GraphRuntimeState(variable_pool=variable_pool, start_at=time.perf_counter())

        # 设置最大执行步骤和时间
        self.max_execution_steps = max_execution_steps
        self.max_execution_time = max_execution_time

    def run(self) -> Generator[GraphEngineEvent, None, None]:
        """
        运行图引擎，处理图的执行逻辑，生成事件。

        该方法会触发图运行开始事件，并根据工作流类型选择相应的流处理器（AnswerStreamProcessor 或 EndStreamProcessor）。
        处理图的执行过程，生成节点运行事件，并在执行过程中捕获异常以生成失败事件。

        :yield: 生成图引擎事件
        """
        # 触发图运行开始事件
        yield GraphRunStartedEvent()

        try:
            # 根据工作流类型选择流处理器
            if self.init_params.workflow_type == WorkflowType.CHAT:
                stream_processor = AnswerStreamProcessor(
                    graph=self.graph, variable_pool=self.graph_runtime_state.variable_pool
                )
            else:
                stream_processor = EndStreamProcessor(
                    graph=self.graph, variable_pool=self.graph_runtime_state.variable_pool
                )

            # 运行图
            generator = stream_processor.process(self._run(start_node_id=self.graph.root_node_id))

            for item in generator:
                try:
                    yield item  # 生成事件
                    if isinstance(item, NodeRunFailedEvent):
                        yield GraphRunFailedEvent(error=item.route_node_state.failed_reason or "Unknown error.")
                        return
                    elif isinstance(item, NodeRunSucceededEvent):
                        if item.node_type == NodeType.END:
                            self.graph_runtime_state.outputs = (
                                item.route_node_state.node_run_result.outputs
                                if item.route_node_state.node_run_result
                                and item.route_node_state.node_run_result.outputs
                                else {}
                            )
                        elif item.node_type == NodeType.ANSWER:
                            if "answer" not in self.graph_runtime_state.outputs:
                                self.graph_runtime_state.outputs["answer"] = ""

                            self.graph_runtime_state.outputs["answer"] += "\n" + (
                                item.route_node_state.node_run_result.outputs.get("answer", "")
                                if item.route_node_state.node_run_result
                                and item.route_node_state.node_run_result.outputs
                                else ""
                            )

                            self.graph_runtime_state.outputs["answer"] = self.graph_runtime_state.outputs[
                                "answer"
                            ].strip()
                except Exception as e:
                    logger.exception("Graph run failed")
                    yield GraphRunFailedEvent(error=str(e))
                    return

            # 触发图运行成功事件
            yield GraphRunSucceededEvent(outputs=self.graph_runtime_state.outputs)
            self._release_thread()  # 释放线程
        except GraphRunFailedError as e:
            yield GraphRunFailedEvent(error=e.error)
            self._release_thread()
            return
        except Exception as e:
            logger.exception("Unknown Error when graph running")
            yield GraphRunFailedEvent(error=str(e))
            self._release_thread()
            raise e

    def _release_thread(self):
        """
        释放线程池，移除主线程池的映射。

        如果当前实例是主线程池，则从全局映射中删除该线程池的ID。
        """
        if self.is_main_thread_pool and self.thread_pool_id in GraphEngine.workflow_thread_pool_mapping:
            del GraphEngine.workflow_thread_pool_mapping[self.thread_pool_id]

    def _run(
        self,
        start_node_id: str,
        in_parallel_id: Optional[str] = None,
        parent_parallel_id: Optional[str] = None,
        parent_parallel_start_node_id: Optional[str] = None,
    ) -> Generator[GraphEngineEvent, None, None]:
        """
        运行图中的节点，处理节点的执行逻辑。

        该方法会根据节点的配置和类型，创建节点实例并执行节点逻辑，处理节点之间的连接和状态更新。

        :param start_node_id: 开始节点的ID
        :param in_parallel_id: 当前并��ID（可选）
        :param parent_parallel_id: 父级并行ID（可选）
        :param parent_parallel_start_node_id: 父级并行开始节点ID（可选）
        :yield: 生成图引擎事件
        """
        parallel_start_node_id = None
        if in_parallel_id:
            parallel_start_node_id = start_node_id

        next_node_id = start_node_id
        previous_route_node_state: Optional[RouteNodeState] = None
        while True:
            # 检查是否达到最大步骤限制
            if self.graph_runtime_state.node_run_steps > self.max_execution_steps:
                raise GraphRunFailedError("Max steps {} reached.".format(self.max_execution_steps))

            # 检查是否达到最大执行时间限制
            if self._is_timed_out(
                start_at=self.graph_runtime_state.start_at, max_execution_time=self.max_execution_time
            ):
                raise GraphRunFailedError("Max execution time {}s reached.".format(self.max_execution_time))

            # 初始化路由节点状态
            route_node_state = self.graph_runtime_state.node_run_state.create_node_state(node_id=next_node_id)

            # 获取节点配置
            node_id = route_node_state.node_id
            node_config = self.graph.node_id_config_mapping.get(node_id)
            if not node_config:
                raise GraphRunFailedError(f"Node {node_id} config not found.")

            # 转换为特定节点类型
            node_type = NodeType(node_config.get("data", {}).get("type"))
            node_cls = node_type_classes_mapping[node_type]

            previous_node_id = previous_route_node_state.node_id if previous_route_node_state else None

            # 初始化工作流运行状态
            node_instance = node_cls(  # type: ignore
                id=route_node_state.id,
                config=node_config,
                graph_init_params=self.init_params,
                graph=self.graph,
                graph_runtime_state=self.graph_runtime_state,
                previous_node_id=previous_node_id,
                thread_pool_id=self.thread_pool_id,
            )

            try:
                # 运行节点
                generator = self._run_node(
                    node_instance=node_instance,
                    route_node_state=route_node_state,
                    parallel_id=in_parallel_id,
                    parallel_start_node_id=parallel_start_node_id,
                    parent_parallel_id=parent_parallel_id,
                    parent_parallel_start_node_id=parent_parallel_start_node_id,
                )

                for item in generator:
                    if isinstance(item, NodeRunStartedEvent):
                        self.graph_runtime_state.node_run_steps += 1
                        item.route_node_state.index = self.graph_runtime_state.node_run_steps

                    yield item  # 生成节点运行事件

                # 更新路由节点状态
                self.graph_runtime_state.node_run_state.node_state_mapping[route_node_state.id] = route_node_state

                # 追加路由
                if previous_route_node_state:
                    self.graph_runtime_state.node_run_state.add_route(
                        source_node_state_id=previous_route_node_state.id, target_node_state_id=route_node_state.id
                    )
            except Exception as e:
                route_node_state.status = RouteNodeState.Status.FAILED
                route_node_state.failed_reason = str(e)
                yield NodeRunFailedEvent(
                    error=str(e),
                    id=node_instance.id,
                    node_id=next_node_id,
                    node_type=node_type,
                    node_data=node_instance.node_data,
                    route_node_state=route_node_state,
                    parallel_id=in_parallel_id,
                    parallel_start_node_id=parallel_start_node_id,
                    parent_parallel_id=parent_parallel_id,
                    parent_parallel_start_node_id=parent_parallel_start_node_id,
                )
                raise e

            # 检查是否为结束节点
            if (
                self.graph.node_id_config_mapping[next_node_id].get("data", {}).get("type", "").lower()
                == NodeType.END.value
            ):
                break

            previous_route_node_state = route_node_state

            # 获取下一个节点ID
            edge_mappings = self.graph.edge_mapping.get(next_node_id)
            if not edge_mappings:
                break

            if len(edge_mappings) == 1:
                edge = edge_mappings[0]

                if edge.run_condition:
                    result = ConditionManager.get_condition_handler(
                        init_params=self.init_params,
                        graph=self.graph,
                        run_condition=edge.run_condition,
                    ).check(
                        graph_runtime_state=self.graph_runtime_state,
                        previous_route_node_state=previous_route_node_state,
                    )

                    if not result:
                        break

                next_node_id = edge.target_node_id
            else:
                final_node_id = None

                if any(edge.run_condition for edge in edge_mappings):
                    # 如果节点有运行条件，根据条件结果获取要执行的节点ID
                    condition_edge_mappings = {}
                    for edge in edge_mappings:
                        if edge.run_condition:
                            run_condition_hash = edge.run_condition.hash
                            if run_condition_hash not in condition_edge_mappings:
                                condition_edge_mappings[run_condition_hash] = []

                            condition_edge_mappings[run_condition_hash].append(edge)

                    for _, sub_edge_mappings in condition_edge_mappings.items():
                        if len(sub_edge_mappings) == 0:
                            continue

                        edge = sub_edge_mappings[0]

                        result = ConditionManager.get_condition_handler(
                            init_params=self.init_params,
                            graph=self.graph,
                            run_condition=edge.run_condition,
                        ).check(
                            graph_runtime_state=self.graph_runtime_state,
                            previous_route_node_state=previous_route_node_state,
                        )

                        if not result:
                            continue

                        if len(sub_edge_mappings) == 1:
                            final_node_id = edge.target_node_id
                        else:
                            parallel_generator = self._run_parallel_branches(
                                edge_mappings=sub_edge_mappings,
                                in_parallel_id=in_parallel_id,
                                parallel_start_node_id=parallel_start_node_id,
                            )

                            for item in parallel_generator:
                                if isinstance(item, str):
                                    final_node_id = item
                                else:
                                    yield item

                        break

                    if not final_node_id:
                        break

                    next_node_id = final_node_id
                else:
                    parallel_generator = self._run_parallel_branches(
                        edge_mappings=edge_mappings,
                        in_parallel_id=in_parallel_id,
                        parallel_start_node_id=parallel_start_node_id,
                    )

                    for item in parallel_generator:
                        if isinstance(item, str):
                            final_node_id = item
                        else:
                            yield item

                    if not final_node_id:
                        break

                    next_node_id = final_node_id

            if in_parallel_id and self.graph.node_parallel_mapping.get(next_node_id, "") != in_parallel_id:
                break

    def _run_parallel_branches(
        self,
        edge_mappings: list[GraphEdge],
        in_parallel_id: Optional[str] = None,
        parallel_start_node_id: Optional[str] = None,
    ) -> Generator[GraphEngineEvent | str, None, None]:
        """
        处理并行分支的执行，使用队列获取结果。

        该方法会为每个边映射创建新的线程，运行并行节点，并通过队列收集结果。

        :param edge_mappings: 边映射列表
        :param in_parallel_id: 当前并行ID（可选）
        :param parallel_start_node_id: 并行开始节点ID（可选）
        :yield: 生成图引擎事件或最终节点ID
        """
        # 如果节点没有运行条件，则并行运行所有节点
        parallel_id = self.graph.node_parallel_mapping.get(edge_mappings[0].target_node_id)
        if not parallel_id:
            node_id = edge_mappings[0].target_node_id
            node_config = self.graph.node_id_config_mapping.get(node_id)
            if not node_config:
                raise GraphRunFailedError(
                    f"Node {node_id} related parallel not found or incorrectly connected to multiple parallel branches."
                )

            node_title = node_config.get("data", {}).get("title")
            raise GraphRunFailedError(
                f"Node {node_title} related parallel not found or incorrectly connected to multiple parallel branches."
            )

        parallel = self.graph.parallel_mapping.get(parallel_id)
        if not parallel:
            raise GraphRunFailedError(f"Parallel {parallel_id} not found.")

        # 运行并行节点，使用队列获取结果
        q: queue.Queue = queue.Queue()

        # 创建一个列表来存储线程
        futures = []

        # 新线程
        for edge in edge_mappings:
            if (
                edge.target_node_id not in self.graph.node_parallel_mapping
                or self.graph.node_parallel_mapping.get(edge.target_node_id, "") != parallel_id
            ):
                continue

            future = self.thread_pool.submit(
                self._run_parallel_node,
                **{
                    "flask_app": current_app._get_current_object(),  # type: ignore[attr-defined]
                    "q": q,
                    "parallel_id": parallel_id,
                    "parallel_start_node_id": edge.target_node_id,
                    "parent_parallel_id": in_parallel_id,
                    "parent_parallel_start_node_id": parallel_start_node_id,
                },
            )

            future.add_done_callback(self.thread_pool.task_done_callback)

            futures.append(future)

        succeeded_count = 0
        while True:
            try:
                event = q.get(timeout=1)
                if event is None:
                    break

                yield event
                if event.parallel_id == parallel_id:
                    if isinstance(event, ParallelBranchRunSucceededEvent):
                        succeeded_count += 1
                        if succeeded_count == len(futures):
                            q.put(None)

                        continue
                    elif isinstance(event, ParallelBranchRunFailedEvent):
                        raise GraphRunFailedError(event.error)
            except queue.Empty:
                continue

        # 等待所有线程完成
        wait(futures)

        # 获取最终节点ID
        final_node_id = parallel.end_to_node_id
        if final_node_id:
            yield final_node_id

    def _run_parallel_node(
        self,
        flask_app: Flask,
        q: queue.Queue,
        parallel_id: str,
        parallel_start_node_id: str,
        parent_parallel_id: Optional[str] = None,
        parent_parallel_start_node_id: Optional[str] = None,
    ) -> None:
        """
        运行并行节点。

        该方法在 Flask 应用上下文中运行指定的并行节点，并将事件放入队列中。

        :param flask_app: Flask 应用实例
        :param q: 用于传递事件的队列
        :param parallel_id: 当前并行ID
        :param parallel_start_node_id: 并行开始节点ID
        :param parent_parallel_id: 父级并行ID（可选）
        :param parent_parallel_start_node_id: 父级并行开始节点ID（可选）
        """
        with flask_app.app_context():
            try:
                q.put(
                    ParallelBranchRunStartedEvent(
                        parallel_id=parallel_id,
                        parallel_start_node_id=parallel_start_node_id,
                        parent_parallel_id=parent_parallel_id,
                        parent_parallel_start_node_id=parent_parallel_start_node_id,
                    )
                )

                # 运行节点
                generator = self._run(
                    start_node_id=parallel_start_node_id,
                    in_parallel_id=parallel_id,
                    parent_parallel_id=parent_parallel_id,
                    parent_parallel_start_node_id=parent_parallel_start_node_id,
                )

                for item in generator:
                    q.put(item)  # 将事件放入队列中

                # 触发图运行成功事件
                q.put(
                    ParallelBranchRunSucceededEvent(
                        parallel_id=parallel_id,
                        parallel_start_node_id=parallel_start_node_id,
                        parent_parallel_id=parent_parallel_id,
                        parent_parallel_start_node_id=parent_parallel_start_node_id,
                    )
                )
            except GraphRunFailedError as e:
                q.put(
                    ParallelBranchRunFailedEvent(
                        parallel_id=parallel_id,
                        parallel_start_node_id=parallel_start_node_id,
                        parent_parallel_id=parent_parallel_id,
                        parent_parallel_start_node_id=parent_parallel_start_node_id,
                        error=e.error,
                    )
                )
            except Exception as e:
                logger.exception("Unknown Error when generating in parallel")
                q.put(
                    ParallelBranchRunFailedEvent(
                        parallel_id=parallel_id,
                        parallel_start_node_id=parallel_start_node_id,
                        parent_parallel_id=parent_parallel_id,
                        parent_parallel_start_node_id=parent_parallel_start_node_id,
                        error=str(e),
                    )
                )
            finally:
                db.session.remove()  # 确保数据库会话被移除

    def _run_node(
        self,
        node_instance: BaseNode,
        route_node_state: RouteNodeState,
        parallel_id: Optional[str] = None,
        parallel_start_node_id: Optional[str] = None,
        parent_parallel_id: Optional[str] = None,
        parent_parallel_start_node_id: Optional[str] = None,
    ) -> Generator[GraphEngineEvent, None, None]:
        """
        运行节点。

        该方法触发节点运行开始事件，执行节点逻辑，并根据执行结果生成相应的事件。

        :param node_instance: 节点实例
        :param route_node_state: 路由节点状态
        :param parallel_id: 当前并行ID（可选）
        :param parallel_start_node_id: 并行开始节点ID（可选）
        :param parent_parallel_id: 父级并行ID（可选）
        :param parent_parallel_start_node_id: 父级并行开始节点ID（可选）
        :yield: 生成图引擎事件
        """
        # 触发节点运行开始事件
        yield NodeRunStartedEvent(
            id=node_instance.id,
            node_id=node_instance.node_id,
            node_type=node_instance.node_type,
            node_data=node_instance.node_data,
            route_node_state=route_node_state,
            predecessor_node_id=node_instance.previous_node_id,
            parallel_id=parallel_id,
            parallel_start_node_id=parallel_start_node_id,
            parent_parallel_id=parent_parallel_id,
            parent_parallel_start_node_id=parent_parallel_start_node_id,
        )

        db.session.close()  # 关闭数据库会话

        try:
            # 运行节点
            generator = node_instance.run()
            for item in generator:
                if isinstance(item, GraphEngineEvent):
                    if isinstance(item, BaseIterationEvent):
                        # 将并行信息添加到迭代事件中
                        item.parallel_id = parallel_id
                        item.parallel_start_node_id = parallel_start_node_id
                        item.parent_parallel_id = parent_parallel_id
                        item.parent_parallel_start_node_id = parent_parallel_start_node_id

                    yield item  # 生成事件
                else:
                    if isinstance(item, RunCompletedEvent):
                        run_result = item.run_result
                        route_node_state.set_finished(run_result=run_result)

                        if run_result.status == WorkflowNodeExecutionStatus.FAILED:
                            yield NodeRunFailedEvent(
                                error=route_node_state.failed_reason or "Unknown error.",
                                id=node_instance.id,
                                node_id=node_instance.node_id,
                                node_type=node_instance.node_type,
                                node_data=node_instance.node_data,
                                route_node_state=route_node_state,
                                parallel_id=parallel_id,
                                parallel_start_node_id=parallel_start_node_id,
                                parent_parallel_id=parent_parallel_id,
                                parent_parallel_start_node_id=parent_parallel_start_node_id,
                            )
                        elif run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED:
                            if run_result.metadata and run_result.metadata.get(NodeRunMetadataKey.TOTAL_TOKENS):
                                # 累加状态总令牌数
                                self.graph_runtime_state.total_tokens += int(
                                    run_result.metadata.get(NodeRunMetadataKey.TOTAL_TOKENS)  # type: ignore[arg-type]
                                )

                            if run_result.llm_usage:
                                # 使用最新的使用情况
                                self.graph_runtime_state.llm_usage += run_result.llm_usage

                            # 将节点输出变量添加到变量池中
                            if run_result.outputs:
                                for variable_key, variable_value in run_result.outputs.items():
                                    # 递归添加变量到变量池
                                    self._append_variables_recursively(
                                        node_id=node_instance.node_id,
                                        variable_key_list=[variable_key],
                                        variable_value=variable_value,
                                    )

                            # 将并行信息添加到运行结果元数据中
                            if parallel_id and parallel_start_node_id:
                                if not run_result.metadata:
                                    run_result.metadata = {}

                                run_result.metadata[NodeRunMetadataKey.PARALLEL_ID] = parallel_id
                                run_result.metadata[NodeRunMetadataKey.PARALLEL_START_NODE_ID] = parallel_start_node_id
                                if parent_parallel_id and parent_parallel_start_node_id:
                                    run_result.metadata[NodeRunMetadataKey.PARENT_PARALLEL_ID] = parent_parallel_id
                                    run_result.metadata[NodeRunMetadataKey.PARENT_PARALLEL_START_NODE_ID] = (
                                        parent_parallel_start_node_id
                                    )

                            yield NodeRunSucceededEvent(
                                id=node_instance.id,
                                node_id=node_instance.node_id,
                                node_type=node_instance.node_type,
                                node_data=node_instance.node_data,
                                route_node_state=route_node_state,
                                parallel_id=parallel_id,
                                parallel_start_node_id=parallel_start_node_id,
                                parent_parallel_id=parent_parallel_id,
                                parent_parallel_start_node_id=parent_parallel_start_node_id,
                            )

                        break
                    elif isinstance(item, RunStreamChunkEvent):
                        yield NodeRunStreamChunkEvent(
                            id=node_instance.id,
                            node_id=node_instance.node_id,
                            node_type=node_instance.node_type,
                            node_data=node_instance.node_data,
                            chunk_content=item.chunk_content,
                            from_variable_selector=item.from_variable_selector,
                            route_node_state=route_node_state,
                            parallel_id=parallel_id,
                            parallel_start_node_id=parallel_start_node_id,
                            parent_parallel_id=parent_parallel_id,
                            parent_parallel_start_node_id=parent_parallel_start_node_id,
                        )
                    elif isinstance(item, RunRetrieverResourceEvent):
                        yield NodeRunRetrieverResourceEvent(
                            id=node_instance.id,
                            node_id=node_instance.node_id,
                            node_type=node_instance.node_type,
                            node_data=node_instance.node_data,
                            retriever_resources=item.retriever_resources,
                            context=item.context,
                            route_node_state=route_node_state,
                            parallel_id=parallel_id,
                            parallel_start_node_id=parallel_start_node_id,
                            parent_parallel_id=parent_parallel_id,
                            parent_parallel_start_node_id=parent_parallel_start_node_id,
                        )
        except GenerateTaskStoppedError:
            # 触发节点运行失败事件
            route_node_state.status = RouteNodeState.Status.FAILED
            route_node_state.failed_reason = "Workflow stopped."
            yield NodeRunFailedEvent(
                error="Workflow stopped.",
                id=node_instance.id,
                node_id=node_instance.node_id,
                node_type=node_instance.node_type,
                node_data=node_instance.node_data,
                route_node_state=route_node_state,
                parallel_id=parallel_id,
                parallel_start_node_id=parallel_start_node_id,
                parent_parallel_id=parent_parallel_id,
                parent_parallel_start_node_id=parent_parallel_start_node_id,
            )
            return
        except Exception as e:
            logger.exception(f"Node {node_instance.node_data.title} run failed")
            raise e
        finally:
            db.session.close()  # 确保数据库会话被关闭

    def _append_variables_recursively(self, node_id: str, variable_key_list: list[str], variable_value: VariableValue):
        """
        递归添加变量。

        该方法将变量添加到变量池中，如果变量值是字典，则递归添加其子变量。

        :param node_id: 节点ID
        :param variable_key_list: 变量键列表
        :param variable_value: 变量值
        """
        self.graph_runtime_state.variable_pool.add([node_id] + variable_key_list, variable_value)

        # 如果变量值是字典，则递归添加变量
        if isinstance(variable_value, dict):
            for key, value in variable_value.items():
                # 构造新的键列表
                new_key_list = variable_key_list + [key]
                self._append_variables_recursively(
                    node_id=node_id, variable_key_list=new_key_list, variable_value=value
                )

    def _is_timed_out(self, start_at: float, max_execution_time: int) -> bool:
        """
        检查是否超时。

        该方法根据开始时间和最大执行时间判断当前是否超时。

        :param start_at: 开始时间
        :param max_execution_time: 最大执行时间
        :return: 超时返回True，否则返回False
        """
        return time.perf_counter() - start_at > max_execution_time

    def create_copy(self):
        """
        创建图引擎的副本。

        该方法返回一个新的图引擎实例，包含一个新的变量池实例。

        :return: 带有新变量池实例的图引擎副本
        """
        new_instance = copy(self)
        new_instance.graph_runtime_state = copy(self.graph_runtime_state)
        new_instance.graph_runtime_state.variable_pool = deepcopy(self.graph_runtime_state.variable_pool)
        return new_instance


class GraphRunFailedError(Exception):
    def __init__(self, error: str):
        self.error = error
