#!/bin/sh
$GOPATH/bin/mockgen -package="testsupport" -self_package="heka/testsupport" -destination="testsupport/mock_net_conn.go" net Conn
$GOPATH/bin/mockgen -package="testsupport" -self_package="heka/testsupport" -destination="testsupport/mock_statsdclient.go" heka/pipeline StatsdClient
$GOPATH/bin/mockgen -package="pipeline"    -self_package="heka/pipeline"    -destination="pipeline/mock_input_test.go" heka/pipeline Input
