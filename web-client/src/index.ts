export { type Transport, type InvokeOptions, type ManagedTransport } from "./transport.js";
export { GosporeClient, type GosporeClientOptions, type EventsAPI, type EventCancel } from "./client.js";
export { HTTPTransport, type HTTPTransportOptions } from "./http.js";
export { WebSocketTransport, type WebSocketTransportOptions, type WebSocketLike, WebSocketFrameConnection } from "./ws.js";
export {
  ManagedFrameTransport,
  type FrameConnection,
  type FrameConnectionState,
  type ManagedFrameTransportOptions,
} from "./frame_transport.js";
export {
  WailsIpcTransport,
  WailsFrameConnection,
  type WailsIpcTransportOptions,
  type WailsBindings,
} from "./wails_transport.js";
export { FrameChannel, SubscriptionIterator, type WithSeqNo, type WireFrame as ChannelWireFrame } from "./channel.js";
export { WireSession, type WireSessionOptions, type AppFrame } from "./session.js";
export { type AuthProvider, withAuth, type AuthedTransport, type ManagedAuthedTransport } from "./auth.js";
export {
  FrameType,
  PayloadEncoding,
  PayloadCompression,
  makeFlags,
  getEncoding,
  getCompression,
  marshalWireFrame,
  unmarshalWireFrame,
  type WireFrame as BinaryWireFrame,
  FrameError,
} from "./binary_frame.js";
export { compress, decompress, type CompressResult } from "./compression.js";
export {
  type TypeKind,
  type TypeDesc,
  type FieldDesc,
  type ObjectDesc,
  type SchemaEntry,
  type BinaryCodecLike,
  type SchemaRegistryLike,
} from "./schema.js";
