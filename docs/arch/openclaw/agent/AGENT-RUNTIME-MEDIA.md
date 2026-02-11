# Agent Runtime 媒体处理

> 本文档详解 Agent Runtime 中图像、音频、视频等媒体的完整处理管线：从用户输入中的图像检测、加载与清洗，到 LLM 响应中的媒体指令解析、TTS 语音合成，以及最终通过各渠道投递给用户。

---

## 目录

- [一、全局视角](#一全局视角)
- [二、图像检测与加载](#二图像检测与加载)
- [三、图像清洗与优化](#三图像清洗与优化)
- [四、Vision 模型能力检测](#四vision-模型能力检测)
- [五、图像分析工具](#五图像分析工具)
- [六、LLM 响应中的媒体指令](#六llm-响应中的媒体指令)
- [七、TTS 语音合成](#七tts-语音合成)
- [八、渠道投递](#八渠道投递)
- [九、沙箱中的媒体路径](#九沙箱中的媒体路径)
- [十、关键常量与限制](#十关键常量与限制)
- [十一、关键源文件索引](#十一关键源文件索引)

---

## 一、全局视角

### 1.1 媒体处理的两个方向

```text
方向 1: 用户 → LLM (输入侧)
┌──────────┐     ┌──────────┐     ┌───────────┐     ┌────────┐
│ 用户消息  │────→│ 图像检测  │────→│ 加载+清洗  │────→│ LLM API│
│ (含图片)  │     │ (4种模式) │     │ (缩放/转码)│     │ (Vision)│
└──────────┘     └──────────┘     └───────────┘     └────────┘

方向 2: LLM → 用户 (输出侧)
┌────────┐     ┌───────────┐     ┌──────────┐     ┌──────────┐
│ LLM 回复│────→│ 媒体指令   │────→│ TTS 合成  │────→│ 渠道投递  │
│ (含     │     │ 解析       │     │ (可选)    │     │ (适配格式)│
│  MEDIA:)│     │ (提取URL)  │     │           │     │          │
└────────┘     └───────────┘     └──────────┘     └──────────┘
```

### 1.2 支持的媒体格式

| 类型 | 支持的格式 | 输出格式 |
| ---- | ---- | ---- |
| 图像 | PNG, JPEG, GIF, WebP, BMP, TIFF, HEIC/HEIF | JPEG (默认), PNG (含透明通道) |
| 音频 | MP3, Opus, OGG, WAV, PCM | MP3 (默认), Opus (Telegram 语音) |
| 视频 | MP4, WebM | 透传（不转码） |

---

## 二、图像检测与加载

### 2.1 四种检测模式

> 源文件: `src/agents/pi-embedded-runner/run/images.ts` — `detectImageReferences()`

用户消息中的图像引用通过四种正则模式检测：

| 模式 | 格式示例 | 场景 |
| ---- | ---- | ---- |
| Media Attached | `[media attached: photo.jpg (image/jpeg)]` | 渠道消息附件 |
| Image Source | `[Image: source: /path/to/image.png]` | 历史消息引用 |
| File URL | `file:///Users/me/photo.jpg` | 文件 URL 引用 |
| File Path | `./screenshot.png`, `~/Desktop/photo.jpg` | 直接路径引用 |

**去重**: 所有检测到的路径经 `realpath` 规范化后放入 `Set`，同一张图片不会被重复加载。

### 2.2 历史消息中的图像扫描

> `detectImagesFromHistory(messages)`

除了当前消息，Runtime 还会扫描历史消息中的图像引用：

```text
历史消息扫描规则:
  • 只扫描 role="user" 的消息
  • 跳过已包含 image content block 的消息 (避免重复加载)
  • 全局去重: 同一图像在多条消息中只注入首次出现的位置
  • 返回带 messageIndex 的引用列表 (用于在正确位置注入)
```

### 2.3 图像加载流程

> `loadImageFromRef()` + `loadWebMedia()`

```text
检测到图像引用 (path / URL)
    │
    ├── 远程 URL? → 拒绝 (仅支持本地文件)
    │
    ├── 沙箱环境?
    │   ├── 路径解析到 sandboxRoot 下
    │   ├── assertSandboxPath() 校验 (无路径穿越、无符号链接)
    │   └── 校验失败 → 尝试 media/inbound/{basename} 回退
    │
    ├── 非沙箱?
    │   └── 路径解析到 workspaceDir 下 (支持 ~ 展开)
    │
    ▼
读取文件 (fs.readFile)
    │
    ▼
MIME 检测 (magic bytes → headers → 扩展名)
    │
    ▼
格式转换:
    ├── HEIC/HEIF → JPEG
    ├── EXIF 方向修正 (Sharp / sips)
    └── 超过大小限制 → 缩放 + 降质量
    │
    ▼
返回 ImageContent { data: Buffer, mimeType: string }
```

### 2.4 `detectAndLoadPromptImages` — 主入口

> 源文件: `src/agents/pi-embedded-runner/run/images.ts`

这是 `runEmbeddedAttempt` 调用的主入口，整合了当前消息和历史消息的图像处理：

```typescript
const imageResult = await detectAndLoadPromptImages({
  prompt: effectivePrompt,      // 用户消息
  workspaceDir,                 // 工作目录
  model: params.model,          // 模型 (检查 vision 能力)
  existingImages: params.images, // 渠道已提供的图片
  historyMessages: session.messages,  // 历史消息
  maxBytes: MAX_IMAGE_BYTES,    // 大小限制
  sandboxRoot,                  // 沙箱根目录 (如有)
});
// imageResult.images → 传给 session.prompt(text, { images })
```

**行为**: 如果模型不支持 vision → 立即返回空结果（不加载任何图像）。

---

## 三、图像清洗与优化

### 3.1 工具结果中的图像清洗

> 源文件: `src/agents/tool-images.ts` — `sanitizeImageBlocks()`

工具执行可能返回截图等大图片，需要在发送给 LLM 之前清洗：

```text
工具结果 content 数组
    │
    ▼
遍历每个 block:
    │
    ├── type !== "image" → 跳过
    │
    ├── 推断 MIME (/9j/ → JPEG, iVBOR → PNG, R0lGOD → GIF)
    │
    ├── resizeImageBase64IfNeeded()
    │   │
    │   ├── 网格搜索: 尺寸 [2000,1800,...,800] × 质量 [85,75,...,35]
    │   ├── 找到第一个 < maxBytes 的组合
    │   └── 转换为 JPEG
    │
    ├── 成功 → 替换为缩放后的 base64
    └── 失败 → 替换为文本错误消息
```

### 3.2 消息历史中的图像清洗

> 源文件: `src/agents/pi-embedded-helpers/images.ts` — `sanitizeSessionMessagesImages()`

在每次 LLM 调用前，历史消息中的图像也需要清洗：

| 消息类型 | 清洗行为 |
| ---- | ---- |
| `toolResult` | 清洗 content 中的图像块 |
| `user` | 清洗 content 数组中的图像块 |
| `assistant` (error) | 清洗 content + 剥离思考签名 |
| `assistant` (正常) | 过滤空文本块 + 清洗图像 |

### 3.3 优化算法

> 源文件: `src/web/media.ts`

```text
图像优化 (网格搜索):
    │
    ├── 检测是否有透明通道 (alpha)
    │   ├── 有 → 先尝试 PNG 优化
    │   │      └── 仍超限 → 降级为 JPEG
    │   └── 无 → 直接 JPEG
    │
    ├── 候选尺寸: [2048, 1536, 1280, 1024, 800]
    ├── 候选质量: [80, 70, 60, 50, 40]
    │
    └── 遍历 尺寸×质量 组合
        → 第一个 < maxBytes 的结果胜出
```

---

## 四、Vision 模型能力检测

> 源文件: `src/agents/pi-embedded-runner/run/images.ts`, `src/agents/model-catalog.ts`

```typescript
function modelSupportsImages(model: { input?: string[] }): boolean {
  return model.input?.includes("image") ?? false;
}
```

**检测逻辑**: 模型定义中的 `input` 数组是否包含 `"image"` 字符串。

**行为**:

| 场景 | 处理 |
| ---- | ---- |
| Vision 模型 + 有图像 | 正常加载图像，注入到 prompt |
| Vision 模型 + 无图像 | 正常文本交互 |
| 非 Vision 模型 + 有图像 | 静默跳过所有图像（不报错） |
| 非 Vision 模型 + 需图像分析 | 使用 `image` 工具（独立视觉模型） |

---

## 五、图像分析工具

### 5.1 image 工具

> 源文件: `src/agents/tools/image-tool.ts`

当用户需要分析图像但主模型不支持 vision 时，LLM 可以使用 `image` 工具委托给专门的视觉模型。

**参数**:

| 参数 | 类型 | 必需 | 说明 |
| ---- | ---- | ---- | ---- |
| `image` | string | 是 | 路径、URL 或 data URL |
| `prompt` | string | 否 | 分析提示 |
| `model` | string | 否 | 覆盖视觉模型 |
| `maxBytesMb` | number | 否 | 覆盖大小限制 |

**工具描述自适应**: 如果主模型自身支持 vision，工具描述会提示 "只在图片未在用户消息中提供时使用"，避免不必要的工具调用。

### 5.2 视觉模型选择

```text
视觉模型解析优先级:
    ├── ① agents.defaults.imageModel (primary + fallbacks)
    ├── ② 提供商自带视觉模型 (如 MiniMax-VL-01)
    ├── ③ 与主模型同提供商的视觉模型
    └── ④ 回退到 OpenAI/Anthropic (如有 Key)
```

**降级链**: 使用 `runWithImageModelFallback()`，与文本模型降级类似但更简单——不检查 Auth Profile 冷却。

### 5.3 执行流程

```text
image({ image: "./screenshot.png", prompt: "描述这张截图" })
    │
    ├── 路径校验 (沙箱: sandbox 路径限制, 非沙箱: ~ 展开)
    ├── 加载图像 (loadWebMedia → 优化)
    ├── 选择视觉模型 (优先级链)
    ├── 调用 complete() (非流式，发送图像 + 文本)
    ├── 降级: 失败 → 下一个候选模型
    └── 返回: { text: "这是一张...", model: "gpt-4o", attempts: [...] }
```

---

## 六、LLM 响应中的媒体指令

### 6.1 MEDIA 令牌格式

> 源文件: `src/media/parse.ts` — `splitMediaFromOutput()`

LLM 回复中可以包含 `MEDIA:` 令牌来引用媒体文件：

```text
支持的格式:
  MEDIA: /absolute/path/to/image.jpg
  MEDIA: ./relative/path/to/chart.png
  MEDIA: https://example.com/image.jpg
  MEDIA: "path with spaces/photo.jpg"
```

**校验规则**:

- HTTP(S) URL: 始终有效
- 本地路径: 必须以 `./` 开头，不能包含 `..`
- 最大长度: 4096 字符
- 代码块内的 `MEDIA:` 被忽略（避免误提取）

### 6.2 解析流程

```text
LLM 输出: "这是分析结果。\nMEDIA: ./charts/output.png\n详细说明如下..."
    │
    ▼  splitMediaFromOutput()
    │
    ├── 提取: mediaUrls = ["./charts/output.png"]
    ├── 清理: text = "这是分析结果。\n详细说明如下..."
    └── 检测: audioAsVoice = false
    │
    ▼  parseReplyDirectives()
    │
    ├── 额外解析: [[reply-to:id]], [[audio_as_voice]]
    └── 返回: { text, mediaUrls, replyToId, audioAsVoice, ... }
```

### 6.3 流式中的媒体提取

在流式输出过程中，EventHandler 通过 `consumePartialReplyDirectives()` 实时提取媒体指令：

```text
LLM delta: "...结果如下\nMEDIA: ./chart.png\n..."
    │
    ▼  consumePartialReplyDirectives()
    │
    ├── 提取 mediaUrls → 附加到 agentEvent
    └── emitAgentEvent({ stream: "assistant", data: { text, mediaUrls } })
```

---

## 七、TTS 语音合成

### 7.1 支持的 TTS 提供商

> 源文件: `src/tts/tts.ts`

| 提供商 | API Key | 默认语音 | 特点 |
| ---- | ---- | ---- | ---- |
| Edge TTS | 不需要 | 自动 | 免费、延迟较高 |
| OpenAI TTS | `OPENAI_API_KEY` | alloy | 高质量、低延迟 |
| ElevenLabs | `ELEVENLABS_API_KEY` | 默认 voice | 最高质量、可克隆 |

### 7.2 自动 TTS 模式

| 模式 | 行为 |
| ---- | ---- |
| `off` | 不生成语音 |
| `always` | 所有回复都生成语音 |
| `inbound` | 仅当用户消息包含音频时生成 |
| `tagged` | 仅当 LLM 输出 `[[tts]]` 标签时生成 |

### 7.3 TTS 指令

LLM 可以通过特殊标签控制 TTS 行为：

```text
[[tts:provider=openai voice=alloy model=gpt-4o-mini-tts]]
  自定义 TTS 参数

[[tts:text]]这里是要朗读的内容[[/tts:text]]
  指定朗读文本 (与显示文本不同)

[[audio_as_voice]]
  将音频作为语音消息发送 (气泡样式)
```

### 7.4 TTS 处理流程

```text
回复 payload 生成
    │
    ▼
maybeApplyTtsToPayload()
    │
    ├── 检查 auto 模式 (off/always/inbound/tagged)
    ├── 解析 TTS 指令 (provider/voice/model 覆盖)
    ├── 文本预处理:
    │   ├── 超过 maxLength → 摘要 (如启用) 或截断
    │   └── 提取 [[tts:text]]...[[/tts:text]] 自定义文本
    │
    ├── textToSpeech()
    │   ├── Edge TTS → 临时 .mp3/.opus 文件
    │   ├── OpenAI TTS → /audio/speech API
    │   └── ElevenLabs → /v1/text-to-speech API
    │
    └── 返回 payload + { mediaUrl: audioPath, audioAsVoice: true/false }
```

### 7.5 渠道输出格式

| 渠道 | 格式 | 语音消息? |
| ---- | ---- | ---- |
| Telegram | Opus (.opus) | 是 (sendVoice) |
| WhatsApp | MP3 (.mp3) | 取决于 ptt 标志 |
| Discord | MP3 (.mp3) | 否 (文件附件) |
| 其他 | MP3 (.mp3) | 否 |

---

## 八、渠道投递

### 8.1 各渠道的媒体投递方式

| 渠道 | 图像 | 音频 | 大小限制 |
| ---- | ---- | ---- | ---- |
| WhatsApp | `image` + caption | `audio` + ptt | 5MB (图), 16MB (音视频) |
| Telegram | `sendPhoto` + caption | `sendVoice` (Opus) / `sendAudio` | 10MB (图), 50MB (文件) |
| Discord | 文件附件 + content | 文件附件 | 25MB |
| iMessage | `--file` 附件 | `--file` 附件 | 16MB |
| Signal | 附件 | 附件 | - |

### 8.2 媒体加载管线

```text
MEDIA URL/路径 (从 LLM 回复中提取)
    │
    ▼
loadWebMedia()
    │
    ├── 检测媒体类型 (image/audio/video/document)
    ├── 应用渠道大小限制
    ├── 图像优化 (缩放/转码)
    │
    └── 返回 { buffer, contentType, kind, fileName }
```

### 8.3 caption 处理

- Telegram: caption 最大 1024 字符，超出部分作为独立文本消息发送
- WhatsApp: caption 与图像一起发送
- Discord: content 字段 + 文件附件
- 无 caption 时: 图像单独发送

---

## 九、沙箱中的媒体路径

### 9.1 路径校验

> 源文件: `src/agents/sandbox-paths.ts`

沙箱内的图像路径受严格校验：

```text
assertSandboxPath(filePath, cwd, sandboxRoot)
    │
    ├── 解析路径 (相对 cwd → 绝对路径)
    ├── 计算相对于 sandboxRoot 的路径
    ├── 检查: 不能以 .. 开头 (路径穿越)
    ├── 检查: 不能是绝对路径
    └── 逐级检查: 路径中无符号链接
```

### 9.2 回退机制

如果直接路径校验失败，image 工具会尝试 `media/inbound/{basename}` 回退路径：

```text
image({ image: "/tmp/photo.jpg" })
    │
    ├── assertSandboxPath() → 失败 (路径在沙箱外)
    │
    ▼  回退尝试
    │
    └── 检查: sandboxRoot/media/inbound/photo.jpg 是否存在?
        ├── 存在 → 使用这个路径
        └── 不存在 → 返回错误
```

---

## 十、关键常量与限制

| 常量 | 值 | 说明 |
| ---- | ---- | ---- |
| `MAX_IMAGE_BYTES` | 5 MB | 图像大小上限 |
| `MAX_IMAGE_DIMENSION_PX` | 2000 px | 图像最大边长 |
| 优化候选尺寸 | 2048, 1536, 1280, 1024, 800 | 网格搜索尺寸 |
| 优化候选质量 | 80, 70, 60, 50, 40 | JPEG 质量 |
| `MEDIA:` 最大长度 | 4096 字符 | 媒体令牌 URL/路径上限 |
| TTS 最大文本 | 4096 字符 | textToSpeech 输入上限 |
| TTS 推荐长度 | 1500 字符 | 超出时摘要或截断 |
| Embedding 图像格式 | JPEG (默认) | 输出格式 |

**可配置**: `agents.defaults.mediaMaxMb` 可覆盖默认大小限制。

---

## 十一、关键源文件索引

| 文件 | 核心功能 |
| ---- | ---- |
| `src/agents/pi-embedded-runner/run/images.ts` | 图像检测、加载、注入主入口 |
| `src/agents/tool-images.ts` | 工具结果图像清洗 (`sanitizeImageBlocks`) |
| `src/agents/pi-embedded-helpers/images.ts` | 消息历史图像清洗 (`sanitizeSessionMessagesImages`) |
| `src/agents/tools/image-tool.ts` | `image` 工具：视觉模型分析 |
| `src/agents/model-catalog.ts` | Vision 能力检测 (`modelSupportsVision`) |
| `src/web/media.ts` | 媒体加载 + 优化 (`loadWebMedia`) |
| `src/media/parse.ts` | `MEDIA:` 令牌解析 (`splitMediaFromOutput`) |
| `src/media/image-ops.ts` | EXIF 方向修正、图像处理 |
| `src/auto-reply/reply/reply-directives.ts` | 回复指令解析 (`parseReplyDirectives`) |
| `src/tts/tts.ts` | TTS 合成 (`textToSpeech`, `maybeApplyTtsToPayload`) |
| `src/agents/sandbox-paths.ts` | 沙箱路径校验 (`assertSandboxPath`) |
| `src/agents/model-fallback.ts` | 图像模型降级 (`runWithImageModelFallback`) |
