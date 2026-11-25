
import { useRef, useState } from "react";
import {
  Alert,
  Box,
  Button,
  CircularProgress,
  CssBaseline,
  Stack,
  TextField,
  ThemeProvider,
  createTheme,
} from "@mui/material";

import Canvas from "./components/Canvas";
import NodeEditor from "./components/NodeEditor";
import NodePalette from "./components/NodePalette";
import { useCanvasStore } from "./store/useStore";

type WorkflowApiResponse =
  | {
      success: true;
      run_id: string;
      conversation_id: string;
      status: string;
    }
  | {
      success: false;
      message: string;
    };

interface RunStatusResponse {
  success: boolean;
  run_id: string;
  status: string;
  result?: {
    content?: string;
    model?: string;
  } | null;
  error?: string;
  message?: string;
}

// Go 后端监听 8080 端口
const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8080";

const sleep = (ms: number) =>
  new Promise((resolve) => setTimeout(resolve, ms));

// 轮询 Go 后端，直到工作流执行完成
async function pollRunResult(runId: string): Promise<RunStatusResponse> {
  const maxAttempts = 120;

  for (let i = 0; i < maxAttempts; i++) {
    await sleep(1000);

    const response = await fetch(
      `${API_BASE_URL}/api/workflow/run/${runId}`,
    );

    const data = (await response.json()) as RunStatusResponse;

    if (!response.ok || !data.success) {
      throw new Error(data.message ?? "查询工作流状态失败");
    }

    if (data.status === "succeeded") {
      return data;
    }

    if (data.status === "failed") {
      throw new Error(data.error || "工作流执行失败");
    }

    console.log("Workflow status:", data.status);
  }

  throw new Error("工作流执行超时，请检查后端日志");
}

function App() {
  const { isDarkMode, saveFlow } = useCanvasStore();

  const [workflowInput, setWorkflowInput] = useState("你好");

  const [conversationId, setConversationId] = useState(
    () => crypto.randomUUID(),
  );

  const [isRunning, setIsRunning] = useState(false);

  // 保存当前逻辑请求的幂等 Key
  // 网络异常后再次提交时，复用之前的 Key
  const idempotencyKeyRef = useRef<string | null>(null);

  const [runMessage, setRunMessage] = useState<{
    type: "success" | "error";
    content: string;
  } | null>(null);

  const handleRun = async () => {
    setIsRunning(true);
    setRunMessage(null);

    try {
      // 1. 获取当前画布工作流
      const savedFlow = saveFlow();

      // 2. 获取或生成幂等 Key
      // 同一次逻辑请求重试时，不重新生成
      if (!idempotencyKeyRef.current) {
        idempotencyKeyRef.current = crypto.randomUUID();
      }

      const idempotencyKey = idempotencyKeyRef.current;

      // 3. 向 Go 后端提交工作流
      const response = await fetch(`${API_BASE_URL}/api/workflow/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey,
        },
        body: JSON.stringify({
          flow: savedFlow,
          input: workflowInput,
          conversationId,
        }),
      });

      const data = (await response.json()) as WorkflowApiResponse;

      if (!response.ok || !data.success) {
        // 明确的客户端错误，说明需要修正请求
        // 4xx 可以重新生成 Key
        // 5xx 保留 Key，因为服务端可能已经提交成功
        if (response.status >= 400 && response.status < 500) {
          idempotencyKeyRef.current = null;
        }

        throw new Error(
          data.success ? "工作流提交失败" : data.message,
        );
      }

      if (!data.run_id) {
        throw new Error("Go 后端响应缺少 run_id");
      }

      // 已经拿到 Run ID，后端确认了提交
      // 下一次主动提交属于新的逻辑请求
      idempotencyKeyRef.current = null;

      console.log("Workflow Submitted:", data);

      // 5. 提示用户正在执行
      setRunMessage({
        type: "success",
        content: `工作流提交成功，正在等待 DeepSeek 回复...\nRun ID: ${data.run_id}`,
      });

      // 6. 轮询 Go 后端，等待执行结果
      const result = await pollRunResult(data.run_id);

      // 7. 显示大模型最终回复
      setRunMessage({
        type: "success",
        content: `${result.result?.content ?? "模型没有返回文本"}\n\n模型: ${
          result.result?.model ?? "未知"
        }`,
      });

      console.log("Workflow Result:", result);
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Workflow execution failed";

      setRunMessage({
        type: "error",
        content: message,
      });

      console.error("Workflow Error:", error);
    } finally {
      setIsRunning(false);
    }
  };

  const handleNewConversation = () => {
    setConversationId(crypto.randomUUID());

    // 新会话属于新的逻辑请求
    idempotencyKeyRef.current = null;

    setRunMessage(null);
  };

  const theme = createTheme({
    palette: {
      mode: isDarkMode ? "dark" : "light",
    },
  });

  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />

      <div style={{ display: "flex", height: "100vh" }}>
        <NodePalette />

        <div style={{ flex: 1, position: "relative" }}>
          <Box
            sx={{
              position: "absolute",
              top: 20,
              right: 20,
              zIndex: 10,
              width: 340,
            }}
          >
            <Stack spacing={1}>
              <TextField
                size="small"
                label="工作流输入"
                value={workflowInput}
                onChange={(event) =>
                  setWorkflowInput(event.target.value)
                }
                disabled={isRunning}
              />

              <Button
                variant="contained"
                onClick={handleRun}
                disabled={isRunning}
                startIcon={
                  isRunning ? (
                    <CircularProgress size={16} color="inherit" />
                  ) : null
                }
              >
                {isRunning ? "运行中..." : "Run Workflow"}
              </Button>

              <Button
                variant="outlined"
                onClick={handleNewConversation}
                disabled={isRunning}
              >
                新建会话
              </Button>

              {runMessage && (
                <Alert
                  severity={runMessage.type}
                  sx={{
                    maxHeight: 300,
                    overflow: "auto",
                    whiteSpace: "pre-wrap",
                    wordBreak: "break-word",
                  }}
                >
                  {runMessage.content}
                </Alert>
              )}
            </Stack>
          </Box>

          <Canvas />
          <NodeEditor />
        </div>
      </div>
    </ThemeProvider>
  );
}

export default App;
