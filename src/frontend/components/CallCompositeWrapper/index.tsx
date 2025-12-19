"use client";

import React, { useMemo, useEffect, useState } from 'react';
import { CallComposite, CallAdapter, useAzureCommunicationCallAdapter } from '@azure/communication-react';
import { AzureCommunicationTokenCredential, CommunicationUserIdentifier } from '@azure/communication-common';
import type { RoomCallLocator } from '@azure/communication-calling';
import { startLocal60fpsCapture, StopFn } from '@/utils/micro';


export type CallCompositeWrapperProps = {
  roomId: string;
  acsUserId: string;
  acsToken: string;
  displayName?: string;
};

const leaveCall = async (adapter: CallAdapter): Promise<void> => {
  await adapter.leaveCall().catch((e: unknown) => {
    console.error('Failed to leave call', e);
  });
};

export default function CallCompositeWrapper({
  roomId,
  acsUserId,
  acsToken,
  displayName = 'User'
}: CallCompositeWrapperProps) {

  const credential = useMemo(() => {
    try {
      return new AzureCommunicationTokenCredential(acsToken);
    } catch {
      console.error('Failed to construct token credential');
      return undefined;
    }
  }, [acsToken]);

  const roomLocator = useMemo<RoomCallLocator>(() => ({
    roomId
  }), [roomId]);

  const userIdObj = useMemo<CommunicationUserIdentifier>(() => ({
    communicationUserId: acsUserId
  }), [acsUserId]);

  const [stopCapture, setStopCapture] = useState<StopFn | null>(null);
  const sessionId = useMemo(() => crypto.randomUUID(), []);
  const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? 'http://localhost:9000';
  const WS_URL = `${API_BASE.replace(/^http/, 'ws')}/micro/ws?sessionId=${sessionId}&participantId=local`;

  const adapter = useAzureCommunicationCallAdapter(
    {
      userId: userIdObj,
      displayName,
      credential,
      locator: roomLocator
    },
    undefined,
    leaveCall
  );

  useEffect(() => {
    if (!adapter) return;
    let disposed = false;

    const tryStart = async () => {
      const s = adapter.getState();
      // Prefer checking for a rendered local video stream view target.
      const localStream = s.call?.localVideoStreams?.find(st => st.mediaStreamType === 'Video');
      const isRendered = !!(localStream?.view?.target);
      const selectedDeviceId = s.devices?.selectedCamera?.id;
      if (!isRendered || stopCapture) return;

      const videoEl = localStream?.view?.target as HTMLVideoElement | undefined;

      const stop = await startLocal60fpsCapture({
        sessionId,
        participantId: 'local',
        wsUrl: WS_URL,
        deviceId: selectedDeviceId,
        inputVideoElement: videoEl
      });
      if (!disposed) setStopCapture(() => stop);
    };

    const unsub = adapter.onStateChange(() => {
      const s = adapter.getState();
      const localStream = s.call?.localVideoStreams?.find(st => st.mediaStreamType === 'Video');
      const isRendered = !!(localStream?.view?.target);
      if (isRendered && !stopCapture) tryStart();
      if (!isRendered && stopCapture) { stopCapture(); setStopCapture(null); }
    });

    // If camera is already rendered, start now
    tryStart();

    return () => {
      disposed = true;
      try { if (typeof (unsub as unknown) === 'function') (unsub as unknown as () => void)(); } catch {}
          if (stopCapture) stopCapture();
          setStopCapture(null);
    };
  }, [adapter, WS_URL, stopCapture, sessionId]);

  if (!adapter) {
    return <div className="p-6 text-blue-600">Joining room...</div>;
  }

  if (!credential) {
    return <div className="p-6 text-red-600">Failed to construct credential. Provided token is malformed.</div>;
  }

  return (
    <div style={{ height: '100vh', width: '100vw' }}>
      <CallComposite adapter={adapter} />
    </div>
  );
}
