import { Test, TestingModule } from '@nestjs/testing';
import { PeerNetworkService } from './peer_network.service';

describe('PeerNetworkService', () => {
  let service: PeerNetworkService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PeerNetworkService],
    }).compile();

    service = module.get<PeerNetworkService>(PeerNetworkService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
