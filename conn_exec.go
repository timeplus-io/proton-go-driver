// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package proton

import (
	"context"
	"github.com/timeplus-io/proton-go-driver/v2/lib/proto"
	"time"
)

func (c *connect) exec(ctx context.Context, query string, args ...interface{}) error {
	var (
		options                    = queryOptions(ctx)
		queryParamsProtocolSupport = c.revision >= proto.DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS
		body, err                  = bindQueryOrAppendParameters(queryParamsProtocolSupport, &options, query, c.server.Timezone, args...)
	)
	if err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok {
		c.conn.SetDeadline(deadline)
		defer c.conn.SetDeadline(time.Time{})
	}
	if err := c.sendQuery(body, &options); err != nil {
		return err
	}
	return c.process(ctx, options.onProcess())
}
