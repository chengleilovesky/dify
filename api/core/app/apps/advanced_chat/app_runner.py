import logging
from collections.abc import Mapping
from typing import Any, cast

from sqlalchemy import select
from sqlalchemy.orm import Session

from configs import dify_config
from core.app.apps.advanced_chat.app_config_manager import AdvancedChatAppConfig
from core.app.apps.base_app_queue_manager import AppQueueManager
from core.app.apps.workflow_app_runner import WorkflowBasedAppRunner
from core.app.entities.app_invoke_entities import AdvancedChatAppGenerateEntity, InvokeFrom
from core.app.entities.queue_entities import (
    QueueAnnotationReplyEvent,
    QueueStopEvent,
    QueueTextChunkEvent,
)
from core.moderation.base import ModerationError
from core.workflow.callbacks import WorkflowCallback, WorkflowLoggingCallback
from core.workflow.entities.variable_pool import VariablePool
from core.workflow.enums import SystemVariableKey
from core.workflow.workflow_entry import WorkflowEntry
from extensions.ext_database import db
from models.enums import UserFrom
from models.model import App, Conversation, EndUser, Message
from models.workflow import ConversationVariable, WorkflowType

logger = logging.getLogger(__name__)


class AdvancedChatAppRunner(WorkflowBasedAppRunner):
    """
    AdvancedChat Application Runner
    该类负责运行高级聊天应用程序，处理聊天工作流的执行逻辑。
    """

    def __init__(
        self,
        application_generate_entity: AdvancedChatAppGenerateEntity,
        queue_manager: AppQueueManager,
        conversation: Conversation,
        message: Message,
    ) -> None:
        """
        初始化 AdvancedChatAppRunner 实例。

        :param application_generate_entity: 应用生成实体，包含应用的配置信息和输入数据。
        :param queue_manager: 应用队列管理器，负责管理工作流的执行队列。
        :param conversation: 当前对话对象，包含对话的相关信息。
        :param message: 当前消息对象，包含消息的相关信息。
        """
        super().__init__(queue_manager)  # 调用父类构造函数
        self.application_generate_entity = application_generate_entity
        self.conversation = conversation
        self.message = message

    def run(self) -> None:
        """
        运行高级聊天应用程序。

        该方法负责获取应用配置、用户信息、工作流信息，并初始化工作流执行所需的变量池和图结构。
        最后，调用工作流的执行入口，处理生成的事件。
        """
        # 获取应用配置并进行类型转换
        app_config = self.application_generate_entity.app_config
        app_config = cast(AdvancedChatAppConfig, app_config)

        # 查询应用记录
        app_record = db.session.query(App).filter(App.id == app_config.app_id).first()
        if not app_record:
            raise ValueError("App not found")  # 如果未找到应用，抛出异常

        # 获取工作流
        workflow = self.get_workflow(app_model=app_record, workflow_id=app_config.workflow_id)
        if not workflow:
            raise ValueError("Workflow not initialized")  # 如果工作流未初始化，抛出异常

        user_id = None
        # 根据调用来源获取用户ID
        if self.application_generate_entity.invoke_from in {InvokeFrom.WEB_APP, InvokeFrom.SERVICE_API}:
            end_user = db.session.query(EndUser).filter(EndUser.id == self.application_generate_entity.user_id).first()
            if end_user:
                user_id = end_user.session_id
        else:
            user_id = self.application_generate_entity.user_id

        # 初始化工作流回调列表
        workflow_callbacks: list[WorkflowCallback] = []
        if dify_config.DEBUG:
            workflow_callbacks.append(WorkflowLoggingCallback())  # 在调试模式下添加日志回调

        if self.application_generate_entity.single_iteration_run:
            # 如果请求单次迭代运行
            graph, variable_pool = self._get_graph_and_variable_pool_of_single_iteration(
                workflow=workflow,
                node_id=self.application_generate_entity.single_iteration_run.node_id,
                user_inputs=self.application_generate_entity.single_iteration_run.inputs,
            )
        else:
            # 获取输入和文件
            inputs = self.application_generate_entity.inputs
            query = self.application_generate_entity.query
            files = self.application_generate_entity.files

            # 处理输入的审查
            if self.handle_input_moderation(
                app_record=app_record,
                app_generate_entity=self.application_generate_entity,
                inputs=inputs,
                query=query,
                message_id=self.message.id,
            ):
                return  # 如果输入被审查，返回

            # 处理注释回复
            if self.handle_annotation_reply(
                app_record=app_record,
                message=self.message,
                query=query,
                app_generate_entity=self.application_generate_entity,
            ):
                return  # 如果有注释回复，返回

            # 初始化对话变量
            stmt = select(ConversationVariable).where(
                ConversationVariable.app_id == self.conversation.app_id,
                ConversationVariable.conversation_id == self.conversation.id,
            )
            with Session(db.engine) as session:
                conversation_variables = session.scalars(stmt).all()
                if not conversation_variables:
                    # 如果对话变量不存在，则创建
                    conversation_variables = [
                        ConversationVariable.from_variable(
                            app_id=self.conversation.app_id, conversation_id=self.conversation.id, variable=variable
                        )
                        for variable in workflow.conversation_variables
                    ]
                    session.add_all(conversation_variables)  # 添加对话变量到会话
                # 将数据库实体转换为变量
                conversation_variables = [item.to_variable() for item in conversation_variables]
                session.commit()  # 提交会话更改

            # 增加对话计数
            self.conversation.dialogue_count += 1
            conversation_dialogue_count = self.conversation.dialogue_count
            db.session.commit()  # 提交对话计数更改

            # 创建变量池
            system_inputs = {
                SystemVariableKey.QUERY: query,
                SystemVariableKey.FILES: files,
                SystemVariableKey.CONVERSATION_ID: self.conversation.id,
                SystemVariableKey.USER_ID: user_id,
                SystemVariableKey.DIALOGUE_COUNT: conversation_dialogue_count,
                SystemVariableKey.APP_ID: app_config.app_id,
                SystemVariableKey.WORKFLOW_ID: app_config.workflow_id,
                SystemVariableKey.WORKFLOW_RUN_ID: self.application_generate_entity.workflow_run_id,
            }

            # 初始化变量池
            variable_pool = VariablePool(
                system_variables=system_inputs,
                user_inputs=inputs,
                environment_variables=workflow.environment_variables,
                conversation_variables=conversation_variables,
            )

            # 初始化图结构
            graph = self._init_graph(graph_config=workflow.graph_dict)

        db.session.close()  # 关闭数据库会话

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
        )

        # 运行工作流并处理生成的事件
        generator = workflow_entry.run(callbacks=workflow_callbacks)

        for event in generator:
            self._handle_event(workflow_entry, event)  # 处理每个事件

    def handle_input_moderation(
        self,
        app_record: App,
        app_generate_entity: AdvancedChatAppGenerateEntity,
        inputs: Mapping[str, Any],
        query: str,
        message_id: str,
    ) -> bool:
        """
        处理输入的审查。

        :param app_record: 应用记录
        :param app_generate_entity: 应用生成实体
        :param inputs: 输入数据
        :param query: 查询字符串
        :param message_id: 消息ID
        :return: 如果输入被审查，返回True；否则返回False。
        """
        try:
            # 处理敏感词审查
            _, inputs, query = self.moderation_for_inputs(
                app_id=app_record.id,
                tenant_id=app_generate_entity.app_config.tenant_id,
                app_generate_entity=app_generate_entity,
                inputs=inputs,
                query=query,
                message_id=message_id,
            )
        except ModerationError as e:
            # 如果审查失败，发送停止事件并返回
            self._complete_with_stream_output(text=str(e), stopped_by=QueueStopEvent.StopBy.INPUT_MODERATION)
            return True

        return False

    def handle_annotation_reply(
        self, app_record: App, message: Message, query: str, app_generate_entity: AdvancedChatAppGenerateEntity
    ) -> bool:
        """
        处理注释回复。

        :param app_record: 应用记录
        :param message: 消息对象
        :param query: 查询字符串
        :param app_generate_entity: 应用生成实体
        :return: 如果有注释回复，返回True；否则返回False。
        """
        annotation_reply = self.query_app_annotations_to_reply(
            app_record=app_record,
            message=message,
            query=query,
            user_id=app_generate_entity.user_id,
            invoke_from=app_generate_entity.invoke_from,
        )

        if annotation_reply:
            # 如果有注释回复，发布事件并返回
            self._publish_event(QueueAnnotationReplyEvent(message_annotation_id=annotation_reply.id))
            self._complete_with_stream_output(
                text=annotation_reply.content, stopped_by=QueueStopEvent.StopBy.ANNOTATION_REPLY
            )
            return True

        return False

    def _complete_with_stream_output(self, text: str, stopped_by: QueueStopEvent.Stop.By) -> None:
        """
        直接输出文本并发送停止事件。

        :param text: 要输出的文本
        :param stopped_by: 停止事件的来源
        """
        self._publish_event(QueueTextChunkEvent(text=text))  # 发布文本块事件
        self._publish_event(QueueStopEvent(stopped_by=stopped_by))  # 发布停止事件
