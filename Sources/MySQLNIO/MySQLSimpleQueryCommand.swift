import NIOCore

extension MySQLDatabase {
    public func simpleQuery(_ sql: String) -> EventLoopFuture<[MySQLRow]> {
        var rows = [MySQLRow]()
        return self.simpleQuery(sql) { row in
            rows.append(row)
        }.map { rows }
    }
    
    public func simpleQuery(_ sql: String, onRow: @escaping (MySQLRow) -> ()) -> EventLoopFuture<Void> {
        let query = MySQLSimpleQueryCommand(sql: sql, onRow: onRow)
        return self.send(query, logger: self.logger)
    }
}

private final class MySQLSimpleQueryCommand: MySQLCommand {
    let sql: String
    
    enum State {
        case ready
        case columns(count: UInt64)
        case columnsEOF
        case rows
        case done
    }
    var state: State
    
    var columns: [MySQLProtocol.ColumnDefinition41]
    let onRow: (MySQLRow) -> ()
    
    init(sql: String, onRow: @escaping (MySQLRow) -> ()) {
        self.state = .ready
        self.sql = sql
        self.columns = []
        self.onRow = onRow
    }
    
    func handle(packet: inout MySQLPacket, capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        guard !packet.isError else {
            self.state = .done
            let errorPacket = try packet.decode(MySQLProtocol.ERR_Packet.self, capabilities: capabilities)
            let error: Error
            switch errorPacket.errorCode {
            case .DUP_ENTRY:
                error = MySQLError.duplicateEntry(errorPacket.errorMessage)
            case .PARSE_ERROR:
                error = MySQLError.invalidSyntax(errorPacket.errorMessage)
            default:
                error = MySQLError.server(errorPacket)
            }
            throw error
        }
        
        switch self.state {
        case .ready:
            if packet.isOK {
                self.state = .done
                return .done
            } else {
                let res = try packet.decode(MySQLProtocol.COM_QUERY_Response.self, capabilities: capabilities)
                self.state = .columns(count: res.columnCount)
                return .noResponse
            }
        case .columns(let total):
            let column = try packet.decode(MySQLProtocol.ColumnDefinition41.self, capabilities: capabilities)
            self.columns.append(column)
            if self.columns.count == numericCast(total) {
                // We must check BOTH server capabilities AND client defaults.
                // Even if server supports it, if we disabled it in clientDefault, the feature is off.
                let useDeprecateEOF = capabilities.contains(.CLIENT_DEPRECATE_EOF) && 
                                      MySQLProtocol.CapabilityFlags.clientDefault.contains(.CLIENT_DEPRECATE_EOF)
                                      
                if useDeprecateEOF {
                    self.state = .rows
                } else {
                    self.state = .columnsEOF
                }
            }
            return .noResponse
        case .columnsEOF:
             guard packet.isEOF else {
                 throw MySQLError.protocolError
             }
             self.state = .rows
             return .noResponse
        case .rows:
            guard !packet.isEOF else {
                self.state = .done
                return .done
            }
            
            // Check if it's an OK packet acting as EOF (deprecate EOF)
            let useDeprecateEOF = capabilities.contains(.CLIENT_DEPRECATE_EOF) && 
                                  MySQLProtocol.CapabilityFlags.clientDefault.contains(.CLIENT_DEPRECATE_EOF)
                                  
            if packet.isOK && useDeprecateEOF {
                 self.state = .done
                 return .done
            }
            
            let data = try MySQLProtocol.TextResultSetRow.decode(from: &packet, columnCount: columns.count)
            let row = MySQLRow(
                format: .text,
                columnDefinitions: self.columns,
                values: data.values
            )
            self.onRow(row)
            return .noResponse
        case .done: throw MySQLError.protocolError
        }
    }
    
    func activate(capabilities: MySQLProtocol.CapabilityFlags) throws -> MySQLCommandState {
        try .response([
            .encode(MySQLProtocol.COM_QUERY(query: self.sql), capabilities: capabilities)
        ])
    }
}
