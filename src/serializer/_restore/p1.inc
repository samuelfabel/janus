                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Set { key, value },
                    consumed: cursor,
                }
            }
            b"GET" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "GET arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid GET key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Get { key },
                    consumed: cursor,
                }
            }
            b"DEL" | b"DELETE" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "DEL arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid DEL key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Delete { key },
                    consumed: cursor,
                }
            }
            b"EXPIRE" => {
                if arg_count != 3 {
                    return DecodeOutcome::Invalid {
                        message: "EXPIRE arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid EXPIRE key",
                        }
                    }
                };
                let seconds_bytes = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid EXPIRE seconds",
                        }
                    }
                };
                let Ok(seconds) = parse_u64_decimal(seconds_bytes) else {
                    return DecodeOutcome::Invalid {
                        message: "EXPIRE seconds parse",
                    };
                };
                DecodeOutcome::Ok {
                    command: Command::Expire { key, seconds },
                    consumed: cursor,
                }
            }
            b"TTL" => {
                if arg_count != 2 {
                    return DecodeOutcome::Invalid {
                        message: "TTL arity",
                    };
                }
                let key = match read_bulk(input, &mut cursor) {
                    BulkRead::Ok(v) => v,
                    BulkRead::Incomplete => return DecodeOutcome::Incomplete,
                    BulkRead::Invalid => {
                        return DecodeOutcome::Invalid {
                            message: "invalid TTL key",
                        }
                    }
                };
                DecodeOutcome::Ok {
                    command: Command::Ttl { key },
                    consumed: cursor,
                }
            }
            b"SAVE" => {
                if arg_count != 1 {
                    return DecodeOutcome::Invalid {
                        message: "SAVE arity",
                    };
                }
                DecodeOutcome::Ok {
                    command: Command::Save,
                    consumed: cursor,
                }
            }
            b"MULTI" => {
                if arg_count != 1 {
                    return DecodeOutcome::Invalid {
                        message: "MULTI arity",
                    };
