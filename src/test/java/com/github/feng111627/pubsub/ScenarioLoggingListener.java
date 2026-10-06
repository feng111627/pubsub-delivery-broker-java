package com.github.feng111627.pubsub;

import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

import java.util.logging.Logger;

/**
 * 在测试日志中打印每个业务场景（@DisplayName）及其用例结果，
 * 便于从 mvn test 输出直接看出覆盖了哪些场景。
 */
public class ScenarioLoggingListener implements TestExecutionListener {

    private static final Logger LOG = Logger.getLogger(ScenarioLoggingListener.class.getName());

    @Override
    public void testPlanExecutionStarted(TestPlan testPlan) {
        LOG.info("==================== 业务场景测试开始 ====================");
    }

    @Override
    public void executionFinished(TestIdentifier id,
                                  org.junit.platform.engine.TestExecutionResult result) {
        if (!id.isTest()) {
            return;
        }
        String outcome = switch (result.getStatus()) {
            case SUCCESSFUL -> "通过";
            case FAILED -> "失败: " + result.getThrowable().map(Object::toString).orElse("");
            case ABORTED -> "中止";
        };
        LOG.info("[场景] " + id.getDisplayName() + " => " + outcome);
    }

    @Override
    public void testPlanExecutionFinished(TestPlan testPlan) {
        LOG.info("==================== 业务场景测试结束 ====================");
    }
}
