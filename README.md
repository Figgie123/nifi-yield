# nifi-yield

This NiFi processor is used to allow only 1 FlowFile into a bunch of processors. You
place the Yield processor before the bunch, and a Release processor at the end of the
bunch to allow the next FlowFile into the bunch.

## Example

GenerateFlowFile -> Yield -> LookupAttribute -> PutSql -> UpdateAttribute -> Release

This example has 3 processors that need to be done atomically. The Yield/Release
processors will only allow 1 FlowFile into the bunch of 3 processors at a time. Do
not forget to add a secondary Release if your last processor in the bunch has
multiple relationships (ie. success and failure) that are ending the atomic event.

## Deprecation

With the release of NiFi 1.12.x, the Yield/Release process that this provides is
no longer necessary due to the 'Process group FlowFile concurrency' setting in the
Processor Group configuration. Setting that value to 'Single Flowfile per Node' is
basically identical to this Yield/Release process. Because of this new feature,
there will be no more updates to the Yield/Release processors.
