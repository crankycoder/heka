#!/bin/sh

# net.Conn
$GOPATH/bin/mockgen -package="testsupport" \
                    -self_package="heka/testsupport" \
                    -destination="testsupport/mock_net_conn.go" net Conn

# heka.pipeline.Input
$GOPATH/bin/mockgen -package="pipeline" \
                    -self_package="heka/pipeline" \
                    -destination="pipeline/mock_input_test.go" heka/pipeline Input

# heka.pipeline.OutputWriter
$GOPATH/bin/mockgen -package="testsupport" \
                    -self_package="heka/testsupport" \
                    -destination="testsupport/mock_output_writer.go" heka/pipeline OutputWriter



# heka.pipeline.WriteRunner
$GOPATH/bin/mockgen -package="testsupport" \
                    -self_package="heka/testsupport" \
                    -source="pipeline/outputs.go" \
                    -source="pipeline/runner.go" \
                    -destination="testsupport/mock_write_runner.go" heka/pipeline WriteRunner
