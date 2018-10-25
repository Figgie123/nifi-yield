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
