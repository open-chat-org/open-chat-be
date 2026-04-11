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

import { Test, TestingModule } from '@nestjs/testing';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PrismaService } from '../prisma/prisma.service';
import { ServerIdentityService } from '../server_identity/server_identity.service';
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
      ],
    }).compile();

    service = module.get<PeerNetworkService>(PeerNetworkService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
