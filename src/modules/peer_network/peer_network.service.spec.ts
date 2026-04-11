import { Test, TestingModule } from '@nestjs/testing';
import { NetworkTraceService } from '../network_trace/network_trace.service';
import { PeerNetworkService } from './peer_network.service';

describe('PeerNetworkService', () => {
  let service: PeerNetworkService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [NetworkTraceService, PeerNetworkService],
    }).compile();

    service = module.get<PeerNetworkService>(PeerNetworkService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
