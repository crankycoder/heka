#!/bin/sh
$GOPATH/bin/mockgen -package="testsupport" \
                    -self_package="heka/testsupport" \
                    -destination="testsupport/mock_net_conn.go" net Conn

$GOPATH/bin/mockgen -package="pipeline" \
                    -self_package="heka/pipeline" \
                    -destination="pipeline/mock_input_test.go" heka/pipeline Input

$GOPATH/bin/mockgen -package="testsupport" \
                    -self_package="heka/testsupport" \
                    -destination="testsupport/mock_output_writer.go" heka/pipeline OutputWriter

$GOPATH/bin/mockgen -package="testsupport" \
                    -source="../github.com/mozilla-services/heka-mozsvc-plugins/statsdwriter.go" \
                    -destination="testsupport/mock_statsdclient.go" heka_mozsvc_plugins StatsdClient
