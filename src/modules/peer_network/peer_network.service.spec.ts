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

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        NetworkTraceService,
        PeerNetworkService,
        {
          provide: PrismaService,
          useValue: {},
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
});
