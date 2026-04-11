export type NetworkTraceSource = 'direct_message' | 'p2p' | 'realtime';
export type NetworkTraceSeverity = 'error' | 'info' | 'warn';

export type NetworkTraceEvent = {
  details?: unknown;
  event_type: string;
  id: string;
  message_id?: string;
  peer_id?: string;
  session_id?: string;
  severity: NetworkTraceSeverity;
  source: NetworkTraceSource;
  timestamp: string;
};

export type NetworkTraceFilters = {
  event_type?: string;
  message_id?: string;
  peer_id?: string;
  severity?: NetworkTraceSeverity;
  source?: NetworkTraceSource;
};

export type GetRecentTraceEventsInput = NetworkTraceFilters & {
  limit?: number;
};

export type RecordNetworkTraceInput = Omit<NetworkTraceEvent, 'id' | 'timestamp'>;

