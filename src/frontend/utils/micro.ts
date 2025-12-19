export type StopFn = () => Promise<void>;

export type StartOpts = {
  sessionId: string;
  participantId: string; // e.g. "local"
  wsUrl: string;         // e.g. `${API_BASE.replace('http','ws')}/micro/ws?sessionId=...&participantId=...`
  maxSide?: number;      // longest side downscale (default 256)
  jpegQuality?: number;  // 0..1 (default 0.65)
  onLog?: (s: string) => void;
  deviceId?: string;
  inputStream?: MediaStream;
  inputVideoElement?: HTMLVideoElement;
};

// Minimal local typings for experimental Web APIs used here. Kept intentionally small.
type ImageEncoderEncodedResult = { encodedData: { arrayBuffer(): Promise<ArrayBuffer> } };
type ImageEncoderConstructor = new (options: { type: string; quality: number }) => {
  encode(bitmap: ImageBitmap): Promise<ImageEncoderEncodedResult>;
};

type MediaStreamTrackProcessorConstructor = new (arg: { track: MediaStreamTrack }) => {
  readable: ReadableStream<VideoFrame>;
};

type VideoWithCapture = HTMLVideoElement & {
  captureStream?: (frameRate?: number) => MediaStream;
};

function supportsWebCodecs(): boolean {
  const win = window as unknown as { MediaStreamTrackProcessor?: MediaStreamTrackProcessorConstructor; ImageEncoder?: ImageEncoderConstructor };
  return !!win.MediaStreamTrackProcessor && !!win.ImageEncoder;
}

async function encodeJPEG(frame: VideoFrame, maxSide: number, quality: number): Promise<Uint8Array> {
  const scale = maxSide / Math.max(frame.displayWidth, frame.displayHeight);
  const targetW = Math.max(1, Math.round(frame.displayWidth  * Math.min(1, scale)));
  const targetH = Math.max(1, Math.round(frame.displayHeight * Math.min(1, scale)));
  const bmp = await createImageBitmap(frame, { resizeWidth: targetW, resizeHeight: targetH });
  const win = window as unknown as { ImageEncoder?: ImageEncoderConstructor };
  const EncoderCtor = win.ImageEncoder;
  if (!EncoderCtor) throw new Error('ImageEncoder not available');
  const encoder = new EncoderCtor({ type: 'image/jpeg', quality });
  const encoded = await encoder.encode(bmp);
  const buf = await encoded.encodedData.arrayBuffer();
  bmp.close();
  return new Uint8Array(buf);
}

function buildPacket(frameIndex: number, tsSec: number, mime: string, payload: Uint8Array): ArrayBuffer {
  const enc = new TextEncoder();
  const mimeBytes = enc.encode(mime);
  const headerLen = 4 + 8 + 2 + mimeBytes.length;
  const total = headerLen + payload.byteLength;
  const out = new ArrayBuffer(total);
  const dv = new DataView(out);
  let off = 0;
  dv.setUint32(off, frameIndex); off += 4;
  dv.setFloat64(off, tsSec);     off += 8;
  dv.setUint16(off, mimeBytes.length); off += 2;
  new Uint8Array(out, off, mimeBytes.length).set(mimeBytes); off += mimeBytes.length;
  new Uint8Array(out, off, payload.byteLength).set(payload);
  return out;
}

export async function startLocal60fpsCapture(opts: StartOpts): Promise<StopFn> {
  const maxSide = opts.maxSide ?? 256;
  const quality = opts.jpegQuality ?? 0.65;
  const onLog = opts.onLog ?? (() => { /* no-op */ });

  // Prefer provided inputVideoElement.captureStream(), then inputStream, then getUserMedia (optionally with deviceId)
  let stream: MediaStream | undefined = undefined;
  if (opts.inputVideoElement && typeof (opts.inputVideoElement as VideoWithCapture).captureStream === 'function') {
    try {
      // prefer captureStream from the element (no permission prompt)
      stream = (opts.inputVideoElement as VideoWithCapture).captureStream?.(60);
    } catch (e) {
      const msg = (() => {
        if (e instanceof Error) return e.message;
        return String(e);
      })();
      onLog?.(`captureStream() failed: ${msg}`);
      stream = undefined;
    }
  }

  if (!stream && opts.inputStream) {
    stream = opts.inputStream;
  }

  let createdByGetUserMedia = false;

  if (!stream) {
    // Build candidate constraint sets from strict → relaxed
    const baseConstraints: MediaTrackConstraints = opts.deviceId
      ? { deviceId: { exact: opts.deviceId }, frameRate: { ideal: 60, max: 60 }, width: { ideal: 1280 } }
      : { frameRate: { ideal: 60, max: 60 }, width: { ideal: 1280 }, facingMode: 'user' } as MediaTrackConstraints;

    const candidates: MediaTrackConstraints[] = [];
    candidates.push(baseConstraints);

    // If deviceId was pinned, try without pinning it
    if ('deviceId' in baseConstraints && baseConstraints.deviceId !== undefined) {
      const { deviceId: _d, ...rest } = baseConstraints as MediaTrackConstraints & { deviceId?: unknown };
      void _d; // explicitly ignore
      candidates.push(rest as MediaTrackConstraints);
    }

    // Remove strict max (some browsers/devices dislike max)
    candidates.push({ ...baseConstraints, frameRate: { ideal: 60 } });

    // Try a lower stable FPS
    candidates.push({ ...baseConstraints, frameRate: { ideal: 30, max: 30 } });

    // Finally try with no frameRate constraint
  const { frameRate: _fr, ...noFrame } = baseConstraints as MediaTrackConstraints & { frameRate?: unknown };
  void _fr; // explicitly ignore
  candidates.push(noFrame as MediaTrackConstraints);

  let lastErr: unknown = new Error('getUserMedia failed for all candidates');
    for (const c of candidates) {
      try {
        onLog?.(`trying getUserMedia with constraints: ${JSON.stringify(c)}`);
        stream = await navigator.mediaDevices.getUserMedia({ video: c, audio: false });
        createdByGetUserMedia = true;
        onLog?.('getUserMedia succeeded');
        break;
      } catch (err) {
        lastErr = err;
        // Keep trying next candidate
  const errName = err instanceof Error ? (err.name || err.message) : String(err);
  onLog?.(`getUserMedia candidate failed: ${errName}`);
      }
    }

    if (!stream) {
      // Surface the last error to caller so they can decide; caller should catch (CallCompositeWrapper will).
      throw lastErr;
    }
  }

  // At this point we have a stream
  const tracks = stream.getVideoTracks();
  if (!tracks || tracks.length === 0) throw new Error('No video track available from stream');
  const track = tracks[0];
  try { (track as unknown as { contentHint?: string }).contentHint = 'motion'; } catch {}

  // Open WebSocket (must be ready before pumping frames)
  const ws = new WebSocket(opts.wsUrl);
  ws.binaryType = 'arraybuffer';
  await new Promise<void>((res, rej) => {
    ws.onopen = () => res();
    ws.onerror = () => rej(new Error('WebSocket error'));
    // in some cases onclose can fire quickly; treat as error if closed before open
    ws.onclose = () => rej(new Error('WebSocket closed before open'));
  });

  let running = true;
  let frameIndex = 0;

  // If WebCodecs available, use MediaStreamTrackProcessor for best perf
  if (supportsWebCodecs()) {
    const win = window as unknown as { MediaStreamTrackProcessor?: MediaStreamTrackProcessorConstructor };
    const MediaStreamTrackProcessor = win.MediaStreamTrackProcessor;
    if (MediaStreamTrackProcessor) {
      const processor = new MediaStreamTrackProcessor({ track });
      const reader = processor.readable.getReader();

      const pump = async () => {
        while (running) {
          try {
            const { value, done } = await reader.read();
            if (done || !value) break;
            const vf: VideoFrame = value;
            const ts = (vf.timestamp ?? performance.now() * 1000) / 1e9; // ns→s if present
            try {
              const jpeg = await encodeJPEG(vf, maxSide, quality);
              frameIndex++;
              if (ws.readyState === WebSocket.OPEN) {
                const pkt = buildPacket(frameIndex, ts, 'image/jpeg', jpeg);
                ws.send(pkt);
              }
            } catch {
              // drop frame on error
            } finally {
              try { vf.close(); } catch {}
            }
          } catch {
            // reader.read() errors (stream ended or other); break the loop
            break;
          }
        }
      };

      pump();

      // Return stop function
      return async () => {
        running = false;
        try { reader.releaseLock(); } catch {}
        // Only stop the track if we created it via getUserMedia
        if (createdByGetUserMedia) {
          try { track.stop(); } catch {}
        }
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close();
      };
    }
    // If MediaStreamTrackProcessor is not present despite supportsWebCodecs, fall through to canvas fallback
  }

  // Canvas fallback (may not sustain 60fps)
  const videoEl = document.createElement('video');
  videoEl.muted = true;
  videoEl.playsInline = true;
  // Attach the stream for playback
  videoEl.srcObject = stream;
  try { await videoEl.play(); } catch { /* ignore play failures */ }

  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d')!;
  let rafId: number | null = null;

  const loop = async () => {
    if (!running) return;
    rafId = requestAnimationFrame(loop);
    if (!videoEl.videoWidth || !videoEl.videoHeight) return;
    const scale = maxSide / Math.max(videoEl.videoWidth, videoEl.videoHeight);
    canvas.width = Math.round(videoEl.videoWidth * Math.min(1, scale));
    canvas.height = Math.round(videoEl.videoHeight * Math.min(1, scale));
    try {
      ctx.drawImage(videoEl, 0, 0, canvas.width, canvas.height);
      const ts = performance.now() / 1000;
      const blob: Blob = await new Promise(res => canvas.toBlob(b => res(b as Blob), 'image/jpeg', quality)!);
      const buf = new Uint8Array(await blob.arrayBuffer());
      frameIndex++;
      if (ws.readyState === WebSocket.OPEN) {
        const pkt = buildPacket(frameIndex, ts, 'image/jpeg', buf);
        ws.send(pkt);
      }
    } catch {
      // drop frame on any error
    }
  };

  loop();

  // Return stop function
  return async () => {
    running = false;
    if (rafId !== null) cancelAnimationFrame(rafId);
    // Only stop tracks we created via getUserMedia
    if (createdByGetUserMedia) {
      try { (stream.getTracks() || []).forEach(t => t.stop()); } catch {}
    }
    try { videoEl.pause(); videoEl.srcObject = null; } catch {}
    if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close();
  };
}
