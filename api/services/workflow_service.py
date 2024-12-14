import json
import time
from collections.abc import Sequence
from datetime import datetime, timezone
from typing import Optional

from core.app.apps.advanced_chat.app_config_manager import AdvancedChatAppConfigManager
from core.app.apps.workflow.app_config_manager import WorkflowAppConfigManager
from core.model_runtime.utils.encoders import jsonable_encoder
from core.variables import Variable
from core.workflow.entities.node_entities import NodeRunResult
from core.workflow.errors import WorkflowNodeRunFailedError
from core.workflow.nodes import NodeType
from core.workflow.nodes.event import RunCompletedEvent
from core.workflow.nodes.node_mapping import node_type_classes_mapping
from core.workflow.workflow_entry import WorkflowEntry
from events.app_event import app_draft_workflow_was_synced, app_published_workflow_was_updated
from extensions.ext_database import db
from models.account import Account
from models.enums import CreatedByRole
from models.model import App, AppMode
from models.workflow import (
    Workflow,
    WorkflowNodeExecution,
    WorkflowNodeExecutionStatus,
    WorkflowNodeExecutionTriggeredFrom,
    WorkflowType,
)
from services.errors.app import WorkflowHashNotEqualError
from services.workflow.workflow_converter import WorkflowConverter


class WorkflowService:
    """
    工作流服务类
    负责处理工作流的各种业务逻辑，包括草稿管理、发布、节点执行等
    """

    def get_draft_workflow(self, app_model: App) -> Optional[Workflow]:
        """
        获取应用的草稿工作流
        
        参数:
            app_model: 应用模型实例
        返回:
            如果存在则返回草稿工作流，否则返回 None
        """
        # 通过应用模型查询草稿工作流
        workflow = (
            db.session.query(Workflow)
            .filter(
                Workflow.tenant_id == app_model.tenant_id, 
                Workflow.app_id == app_model.id, 
                Workflow.version == "draft"
            )
            .first()
        )
        return workflow

    def get_published_workflow(self, app_model: App) -> Optional[Workflow]:
        """
        获取应用的已发布工作流
        
        参数:
            app_model: 应用模型实例
        返回:
            如果存在则返回已发布工作流，否则返回 None
        """
        if not app_model.workflow_id:
            return None

        workflow = (
            db.session.query(Workflow)
            .filter(
                Workflow.tenant_id == app_model.tenant_id,
                Workflow.app_id == app_model.id,
                Workflow.id == app_model.workflow_id,
            )
            .first()
        )
        return workflow

    def sync_draft_workflow(
        self,
        *,
        app_model: App,
        graph: dict,
        features: dict,
        unique_hash: Optional[str],
        account: Account,
        environment_variables: Sequence[Variable],
        conversation_variables: Sequence[Variable],
    ) -> Workflow:
        """
        同步草稿工作流
        
        参数:
            app_model: 应用模型实例
            graph: 工作流图数据
            features: 功能配置
            unique_hash: 唯一哈希值，用于版本控制
            account: 账户实例
            environment_variables: 环境变量列表
            conversation_variables: 对话变量列表
        
        返回:
            更新后的工作流实例
        
        异常:
            WorkflowHashNotEqualError: 当工作流哈希不匹配时抛出
        """
        # 获取现有草稿
        workflow = self.get_draft_workflow(app_model=app_model)

        # 检查哈希值是否匹配
        if workflow and workflow.unique_hash != unique_hash:
            raise WorkflowHashNotEqualError()

        # 验证功能结构
        self.validate_features_structure(app_model=app_model, features=features)

        # 如果不存在草稿则创建新的
        if not workflow:
            workflow = Workflow(
                tenant_id=app_model.tenant_id,
                app_id=app_model.id,
                type=WorkflowType.from_app_mode(app_model.mode).value,
                version="draft",
                graph=json.dumps(graph),
                features=json.dumps(features),
                created_by=account.id,
                environment_variables=environment_variables,
                conversation_variables=conversation_variables,
            )
            db.session.add(workflow)
        # 如果存在则更新现有草稿
        else:
            workflow.graph = json.dumps(graph)
            workflow.features = json.dumps(features)
            workflow.updated_by = account.id
            workflow.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
            workflow.environment_variables = environment_variables
            workflow.conversation_variables = conversation_variables

        # 提交数据库更改
        db.session.commit()

        # 触发工作流同步事件
        app_draft_workflow_was_synced.send(app_model, synced_draft_workflow=workflow)

        return workflow

    def publish_workflow(self, app_model: App, account: Account, draft_workflow: Optional[Workflow] = None) -> Workflow:
        """
        发布工作流
        
        参数:
            app_model: 应用模型实例
            account: 账户实例
            draft_workflow: 可选的草稿工作流实例
            
        返回:
            发布后的工作流实例
        """
        if not draft_workflow:
            draft_workflow = self.get_draft_workflow(app_model=app_model)

        if not draft_workflow:
            raise ValueError("No valid workflow found.")

        # 创建新的工作流版本
        workflow = Workflow(
            tenant_id=app_model.tenant_id,
            app_id=app_model.id,
            type=draft_workflow.type,
            version=str(datetime.now(timezone.utc).replace(tzinfo=None)),
            graph=draft_workflow.graph,
            features=draft_workflow.features,
            created_by=account.id,
            environment_variables=draft_workflow.environment_variables,
            conversation_variables=draft_workflow.conversation_variables,
        )

        # 保存到数据库
        db.session.add(workflow)
        db.session.flush()
        db.session.commit()

        # 更新应用的工作流ID
        app_model.workflow_id = workflow.id
        db.session.commit()

        # 触发工作流发布事件
        app_published_workflow_was_updated.send(app_model, published_workflow=workflow)

        return workflow

    def get_default_block_configs(self) -> list[dict]:
        """
        Get default block configs
        """
        # return default block config
        default_block_configs = []
        for node_type, node_class in node_type_classes_mapping.items():
            default_config = node_class.get_default_config()
            if default_config:
                default_block_configs.append(default_config)

        return default_block_configs

    def get_default_block_config(self, node_type: str, filters: Optional[dict] = None) -> Optional[dict]:
        """
        Get default config of node.
        :param node_type: node type
        :param filters: filter by node config parameters.
        :return:
        """
        node_type_enum: NodeType = NodeType(node_type)

        # return default block config
        node_class = node_type_classes_mapping.get(node_type_enum)
        if not node_class:
            return None

        default_config = node_class.get_default_config(filters=filters)
        if not default_config:
            return None

        return default_config

    def run_draft_workflow_node(
        self, app_model: App, node_id: str, user_inputs: dict, account: Account
    ) -> WorkflowNodeExecution:
        """
        运行草稿工作流中的单个节点
        
        参数:
            app_model: 应用模型实例
            node_id: 节点ID
            user_inputs: 用户输入数据
            account: 账户实例
            
        返回:
            节点执行结果实例
        """
        # 获取草稿工作流
        draft_workflow = self.get_draft_workflow(app_model=app_model)
        if not draft_workflow:
            raise ValueError("Workflow not initialized")

        # 记录开始时间
        start_at = time.perf_counter()

        try:
            # 执行节点
            node_instance, generator = WorkflowEntry.single_step_run(
                workflow=draft_workflow,
                node_id=node_id,
                user_inputs=user_inputs,
                user_id=account.id,
            )

            # 处理执行结果
            node_run_result: NodeRunResult | None = None
            for event in generator:
                if isinstance(event, RunCompletedEvent):
                    node_run_result = event.run_result
                    node_run_result.outputs = WorkflowEntry.handle_special_values(node_run_result.outputs)
                    break

            if not node_run_result:
                raise ValueError("Node run failed with no run result")

            # 判断执行状态
            run_succeeded = True if node_run_result.status == WorkflowNodeExecutionStatus.SUCCEEDED else False
            error = node_run_result.error if not run_succeeded else None
        except WorkflowNodeRunFailedError as e:
            node_instance = e.node_instance
            run_succeeded = False
            node_run_result = None
            error = e.error

        # 创建节点执行记录
        workflow_node_execution = WorkflowNodeExecution()
        workflow_node_execution.tenant_id = app_model.tenant_id
        workflow_node_execution.app_id = app_model.id
        workflow_node_execution.workflow_id = draft_workflow.id
        workflow_node_execution.triggered_from = WorkflowNodeExecutionTriggeredFrom.SINGLE_STEP.value
        workflow_node_execution.index = 1
        workflow_node_execution.node_id = node_id
        workflow_node_execution.node_type = node_instance.node_type
        workflow_node_execution.title = node_instance.node_data.title
        workflow_node_execution.elapsed_time = time.perf_counter() - start_at
        workflow_node_execution.created_by_role = CreatedByRole.ACCOUNT.value
        workflow_node_execution.created_by = account.id
        workflow_node_execution.created_at = datetime.now(timezone.utc).replace(tzinfo=None)
        workflow_node_execution.finished_at = datetime.now(timezone.utc).replace(tzinfo=None)

        # 根据执行结果设置状态
        if run_succeeded and node_run_result:
            # 设置成功状态和结果数据
            workflow_node_execution.inputs = json.dumps(node_run_result.inputs) if node_run_result.inputs else None
            workflow_node_execution.process_data = (
                json.dumps(node_run_result.process_data) if node_run_result.process_data else None
            )
            workflow_node_execution.outputs = (
                json.dumps(jsonable_encoder(node_run_result.outputs)) if node_run_result.outputs else None
            )
            workflow_node_execution.execution_metadata = (
                json.dumps(jsonable_encoder(node_run_result.metadata)) if node_run_result.metadata else None
            )
            workflow_node_execution.status = WorkflowNodeExecutionStatus.SUCCEEDED.value
        else:
            # 设置失败状态和错误信息
            workflow_node_execution.status = WorkflowNodeExecutionStatus.FAILED.value
            workflow_node_execution.error = error

        # 保存执行记录
        db.session.add(workflow_node_execution)
        db.session.commit()

        return workflow_node_execution

    def convert_to_workflow(self, app_model: App, account: Account, args: dict) -> App:
        """
        Basic mode of chatbot app(expert mode) to workflow
        Completion App to Workflow App

        :param app_model: App instance
        :param account: Account instance
        :param args: dict
        :return:
        """
        # chatbot convert to workflow mode
        workflow_converter = WorkflowConverter()

        if app_model.mode not in {AppMode.CHAT.value, AppMode.COMPLETION.value}:
            raise ValueError(f"Current App mode: {app_model.mode} is not supported convert to workflow.")

        # convert to workflow
        new_app = workflow_converter.convert_to_workflow(
            app_model=app_model,
            account=account,
            name=args.get("name"),
            icon_type=args.get("icon_type"),
            icon=args.get("icon"),
            icon_background=args.get("icon_background"),
        )

        return new_app

    def validate_features_structure(self, app_model: App, features: dict) -> dict:
        if app_model.mode == AppMode.ADVANCED_CHAT.value:
            return AdvancedChatAppConfigManager.config_validate(
                tenant_id=app_model.tenant_id, config=features, only_structure_validate=True
            )
        elif app_model.mode == AppMode.WORKFLOW.value:
            return WorkflowAppConfigManager.config_validate(
                tenant_id=app_model.tenant_id, config=features, only_structure_validate=True
            )
        else:
            raise ValueError(f"Invalid app mode: {app_model.mode}")
