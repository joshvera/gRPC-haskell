# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: echo.proto for package 'echo'

require 'grpc'
require 'echo'

module Echo
  module Echo

    # TODO: add proto service documentation here
    class Service

      include GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'echo.Echo'

      rpc :DoEcho, EchoRequest, EchoRequest
    end

    Stub = Service.rpc_stub_class
  end
  module Add

    # TODO: add proto service documentation here
    class Service

      include GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'echo.Add'

      rpc :DoAdd, AddRequest, AddResponse
    end

    Stub = Service.rpc_stub_class
  end
end