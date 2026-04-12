jest.mock('../prisma/prisma.service', () => ({
  PrismaService: class PrismaService {},
}));
jest.mock('../server_identity/server_identity.service', () => ({
  ServerIdentityService: class ServerIdentityService {},
}));
jest.mock('./utils/server_score_signature.util', () => ({
  create_server_score_signature: jest.fn(),
  verify_server_score_signature: jest.fn(),
}));
jest.mock('./sync/services/dm_delete_gossip.service', () => ({
  DmDeleteGossipService: class DmDeleteGossipService {},
}));
jest.mock('./sync/services/startup_sync.service', () => ({
  StartupSyncService: class StartupSyncService {},
}));
jest.mock('./sync/services/table_sync_runner.service', () => ({
  TableSyncRunnerService: class TableSyncRunnerService {},
}));

import { Test, TestingModule } from '@nestjs/testing';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PrismaService } from '../prisma/prisma.service';
import { ServerIdentityService } from '../server_identity/server_identity.service';
import { DmDeleteGossipService } from './sync/services/dm_delete_gossip.service';
import { StartupSyncService } from './sync/services/startup_sync.service';
import { TableSyncRunnerService } from './sync/services/table_sync_runner.service';
import { PeerNetworkService } from './peer_network.service';

describe('PeerNetworkService', () => {
  let service: PeerNetworkService;
  let prisma_service_mock: {
    serverNode: {
      findMany: jest.Mock;
    };
  };

  beforeEach(async () => {
    prisma_service_mock = {
      serverNode: {
        findMany: jest.fn().mockResolvedValue([]),
      },
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        NetworkTraceService,
        PeerNetworkService,
        {
          provide: PrismaService,
          useValue: prisma_service_mock,
        },
        {
          provide: ServerIdentityService,
          useValue: {},
        },
        {
          provide: DmDeleteGossipService,
          useValue: {},
        },
        {
          provide: StartupSyncService,
          useValue: {},
        },
        {
          provide: TableSyncRunnerService,
          useValue: {},
        },
      ],
    }).compile();

    service = module.get<PeerNetworkService>(PeerNetworkService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should always include bootstrap peers in desired keep-alive set', async () => {
    const service_any = service as any;
    service_any.keep_alive_bootstrap_peer_ids.add('peer_bootstrap_1');
    service_any.keep_alive_bootstrap_peer_ids.add('peer_bootstrap_2');
    jest
      .spyOn(service_any, 'get_local_peer_id')
      .mockReturnValue('peer_local');
    jest
      .spyOn(service_any, 'select_core_keep_alive_peer_ids')
      .mockResolvedValue([]);

    const desired_peers: Map<string, 'bootstrap' | 'core'> =
      await service_any.build_desired_keep_alive_peers();

    expect(desired_peers.get('peer_bootstrap_1')).toBe('bootstrap');
    expect(desired_peers.get('peer_bootstrap_2')).toBe('bootstrap');
  });

  it('should respect keep-alive core count when selecting core peers', async () => {
    const service_any = service as any;
    service_any.p2p_config.keep_alive_core_count = 2;
    service_any.p2p_config.score_default_max_report_age_seconds = 180;
    jest
      .spyOn(service_any, 'compute_candidate_aggregates')
      .mockResolvedValue([
        {
          last_observed_at_ms: Date.now(),
          mean_score: 92,
          report_count: 5,
          target_peer_id: 'peer_a',
        },
        {
          last_observed_at_ms: Date.now() - 1000,
          mean_score: 88,
          report_count: 4,
          target_peer_id: 'peer_b',
        },
        {
          last_observed_at_ms: Date.now() - 2000,
          mean_score: 80,
          report_count: 3,
          target_peer_id: 'peer_c',
        },
      ]);
    jest.spyOn(service_any, 'get_connected_peers').mockReturnValue([]);
    prisma_service_mock.serverNode.findMany.mockResolvedValue([]);

    const selected_peer_ids: string[] =
      await service_any.select_core_keep_alive_peer_ids(
        'peer_local',
        new Map(),
      );

    expect(selected_peer_ids).toHaveLength(2);
    expect(selected_peer_ids).toEqual(['peer_a', 'peer_b']);
  });
});
