import { Global, Module } from '@nestjs/common';
import { NetworkTraceService } from './network_trace.service';

@Global()
@Module({
  providers: [NetworkTraceService],
  exports: [NetworkTraceService],
})
export class NetworkTraceModule {}

