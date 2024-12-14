import logging
from typing import Optional, cast

from configs import dify_config
from core.app.apps.base_app_queue_manager import AppQueueManager
from core.app.apps.workflow.app_config_manager import WorkflowAppConfig
from core.app.apps.workflow_app_runner import WorkflowBasedAppRunner
from core.app.entities.app_invoke_entities import (
    InvokeFrom,
    WorkflowAppGenerateEntity,
)
from core.workflow.callbacks import WorkflowCallback, WorkflowLoggingCallback
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.enums import SystemVariableKey
from core.workflow.workflow_entry import WorkflowEntry
from extensions.ext_database import db
from models.enums import UserFrom
from models.model import App, EndUser
from models.workflow import WorkflowType

logger = logging.getLogger(__name__)


class WorkflowAppRunner(WorkflowBasedAppRunner):
    """
    Workflow Application Runner
    该类负责运行工作流应用程序，处理工作流的执行逻辑。
    """

    def __init__(
        self,
        application_generate_entity: WorkflowAppGenerateEntity,
        queue_manager: AppQueueManager,
        workflow_thread_pool_id: Optional[str] = None,
    ) -> None:
        """
        初始化 WorkflowAppRunner 实例。

        :param application_generate_entity: 应用生成实体，包含应用的配置信息和输入数据。
        :param queue_manager: 应用队列管理器，负责管理工作流的执行队列。
        :param workflow_thread_pool_id: 工作流线程池ID（可选），用于指定使用的线程池。
        """
        self.application_generate_entity = application_generate_entity
        self.queue_manager = queue_manager
        self.workflow_thread_pool_id = workflow_thread_pool_id

    def run(self) -> None:
        """
        运行工作流应用程序。

        该方法负责获取应用配置、用户信息、工作流信息，并初始化工作流执行所需的变量池和图结构。
        最后，调用工作流的执行入口，处理生成的事件。
        """
        # 获取应用配置并进行类型转换
        app_config = self.application_generate_entity.app_config
        app_config = cast(WorkflowAppConfig, app_config)

        user_id = None
        # 根据调用来源获取用户ID
        if self.application_generate_entity.invoke_from in {InvokeFrom.WEB_APP, InvokeFrom.SERVICE_API}:
            end_user = db.session.query(EndUser).filter(EndUser.id == self.application_generate_entity.user_id).first()
            if end_user:
                user_id = end_user.session_id
        else:
            user_id = self.application_generate_entity.user_id

        # 查询应用记录
        app_record = db.session.query(App).filter(App.id == app_config.app_id).first()
        if not app_record:
            raise ValueError("App not found")  # 如果未找到应用，抛出异常

        # 获取工作流
        workflow = self.get_workflow(app_model=app_record, workflow_id=app_config.workflow_id)
        if not workflow:
            raise ValueError("Workflow not initialized")  # 如果工作流未初始化，抛出异常

        db.session.close()  # 关闭数据库会话

        # 初始化工作流回调列表
        workflow_callbacks: list[WorkflowCallback] = []
        if dify_config.DEBUG:
            workflow_callbacks.append(WorkflowLoggingCallback())  # 在调试模式下添加日志回调

        # 如果请求单次迭代运行
        if self.application_generate_entity.single_iteration_run:
            graph, variable_pool = self._get_graph_and_variable_pool_of_single_iteration(
                workflow=workflow,
                node_id=self.application_generate_entity.single_iteration_run.node_id,
                user_inputs=self.application_generate_entity.single_iteration_run.inputs,
            )
        else:
            # 获取输入和文件
            inputs = self.application_generate_entity.inputs
            files = self.application_generate_entity.files

            # 创建变量池
            system_inputs = {
                SystemVariableKey.FILES: files,
                SystemVariableKey.USER_ID: user_id,
                SystemVariableKey.APP_ID: app_config.app_id,
                SystemVariableKey.WORKFLOW_ID: app_config.workflow_id,
                SystemVariableKey.WORKFLOW_RUN_ID: self.application_generate_entity.workflow_run_id,
            }

            variable_pool = VariablePool(
                system_variables=system_inputs,
                user_inputs=inputs,
                environment_variables=workflow.environment_variables,
                conversation_variables=[],
            )

            # 初始化图结构
            graph = self._init_graph(graph_config=workflow.graph_dict)

        # 创建工作流入口
        workflow_entry = WorkflowEntry(
            tenant_id=workflow.tenant_id,
            app_id=workflow.app_id,
            workflow_id=workflow.id,
            workflow_type=WorkflowType.value_of(workflow.type),
            graph=graph,
            graph_config=workflow.graph_dict,
            user_id=self.application_generate_entity.user_id,
            user_from=(
                UserFrom.ACCOUNT
                if self.application_generate_entity.invoke_from in {InvokeFrom.EXPLORE, InvokeFrom.DEBUGGER}
                else UserFrom.END_USER
            ),
            invoke_from=self.application_generate_entity.invoke_from,
            call_depth=self.application_generate_entity.call_depth,
            variable_pool=variable_pool,
            thread_pool_id=self.workflow_thread_pool_id,
        )

        # 运行工作流并处理生成的事件
        generator = workflow_entry.run(callbacks=workflow_callbacks)

        for event in generator:
            self._handle_event(workflow_entry, event)  # 处理每个事件
