#region license
// Transformalize
// Configurable Extract, Transform, and Load
// Copyright 2013-2017 Dale Newman
//  
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   
//       http://www.apache.org/licenses/LICENSE-2.0
//   
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Autofac;
using RethinkDb;
using RethinkDb.ConnectionFactories;
using Transformalize.Configuration;
using Transformalize.Context;
using Transformalize.Contracts;
using Transformalize.Nulls;
using Transformalize.Providers.RethinkDB;

namespace Transformalize.Providers.RethinkDb.Autofac {

    public class RethinkDbModule : Module {

        private const string Provider = "rethinkdb";

        protected override void Load(ContainerBuilder builder) {

            if (!builder.Properties.ContainsKey("Process")) {
                return;
            }

            var process = (Process)builder.Properties["Process"];

            // connections
            foreach (var connection in process.Connections.Where(c => c.Provider == Provider)) {

                builder.Register<ISchemaReader>(ctx => new NullSchemaReader()).Named<ISchemaReader>(connection.Key);

                builder.Register<IConnectionFactory>(ctx =>
                {
                    if (connection.Servers.Any()) {
                        var endPoints = new List<EndPoint>();
                        foreach (var server in connection.Servers) {
                            endPoints.Add(GetEndPoint(server.Name, server.Port));
                        }
                        return new ConnectionPoolingConnectionFactory(new ReliableConnectionFactory(new DefaultConnectionFactory(endPoints)), TimeSpan.FromSeconds(connection.RequestTimeout));
                    }

                    return new ReliableConnectionFactory(new DefaultConnectionFactory(new List<EndPoint> { GetEndPoint(connection.Server, connection.Port) }));
                }).Named<IConnectionFactory>(connection.Key).SingleInstance();
            }

            // Entity input
            foreach (var entity in process.Entities.Where(e => process.Connections.First(c => c.Name == e.Connection).Provider == Provider)) {

                // input version detector
                builder.RegisterType<NullInputProvider>().Named<IInputProvider>(entity.Key);

                // input reader
                builder.Register<IRead>(ctx => new NullReader(ctx.ResolveNamed<InputContext>(entity.Key), false)).Named<IRead>(entity.Key);
            }

            if (process.Output().Provider == Provider) {
                // PROCESS OUTPUT CONTROLLER
                builder.Register<IOutputController>(ctx => new NullOutputController()).As<IOutputController>();

                foreach (var entity in process.Entities) {
                    builder.Register<IOutputController>(ctx => {
                        var input = ctx.ResolveNamed<InputContext>(entity.Key);
                        var output = ctx.ResolveNamed<OutputContext>(entity.Key);
                        var factory = ctx.ResolveNamed<IConnectionFactory>(output.Connection.Key);
                        var initializer = process.Mode == "init" ? (IAction)new RethinkDbInitializer(input, output, factory) : new NullInitializer();
                        return new RethinkDbOutputController(
                            output,
                            initializer,
                            ctx.ResolveNamed<IInputProvider>(entity.Key),
                            new RethinkDbOutputProvider(input, output, factory),
                            factory
                        );
                    }
                    ).Named<IOutputController>(entity.Key);

                    // ENTITY WRITER
                    builder.Register<IWrite>(ctx => {
                        var output = ctx.ResolveNamed<OutputContext>(entity.Key);
                        return new RethinkDbWriter(
                            ctx.ResolveNamed<InputContext>(entity.Key),
                            output,
                            ctx.ResolveNamed<IConnectionFactory>(output.Connection.Key)
                        );
                    }).Named<IWrite>(entity.Key);
                }
            }

        }

        private EndPoint GetEndPoint(string nameOrAddress, int port) {
            if (IPAddress.TryParse(nameOrAddress, out var ip))
                return new IPEndPoint(ip, port);
            return new DnsEndPoint(nameOrAddress, port);
        }
    }
}