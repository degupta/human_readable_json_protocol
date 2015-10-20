require 'thrift'
require 'json'

class HumanReadableJsonProtocol < Thrift::BaseProtocol
  METHOD_KEY         = "method"
  SERVICES_KEY       = "services"
  NAME_KEY           = "name"
  ARGUMENTS_KEY      = "arguments"
  RESULT_KEY         = "result"
  SUCCESS_KEY        = "success"
  EXCEPTIONS_KEY     = "exceptions"
  EXCEPTION_KEY      = "exception"
  FUNCTIONS_KEY      = "functions"
  KEY_KEY            = "key"
  ONEWAY_KEY         = "oneway"
  FIELDS_KEY         = "fields"
  STRUCTS_KEY        = "structs"
  MESSAGE_KEY        = "message"
  CLASS_KEY          = "class"
  TYPE_ID_KEY        = "typeId"
  TYPE_KEY           = "type"
  KEY_TYPE_ID_KEY    = "keyTypeId"
  KEY_TYPE_KEY       = "keyType"
  VALUE_TYPE_ID_KEY  = "valueTypeId"
  VALUE_TYPE_KEY     = "valueType"
  ELEM_TYPE_ID_KEY   = "elemTypeId"
  ELEM_TYPE_KEY      = "elemType"
  RETURN_TYPE_ID_KEY = "returnTypeId"
  RETURN_TYPE_KEY    = "returnType"

  class HumanReadableJsonProtocolFactory < Thrift::BaseProtocolFactory
    def initialize(dir)
      @packages_metadata = []
      Dir.foreach(dir) do |fname|
        if fname.end_with? ".json"
          @packages_metadata << JSON.parse(File.read("#{dir}/#{fname}"))
        end
      end
    end

    def get_protocol(trans, service, debug = false)
      HumanReadableJsonProtocol.new(trans, service, @packages_metadata, debug)
    end
  end

  attr_accessor :metadata

  def initialize(protocol, service, packages_metadata, debug = false)
    super(protocol)
    @oprot = Thrift::JsonProtocol.new(protocol)
    @metadata = packages_metadata
    @service = service
    @params = []
    @err = nil
    @debug = debug
    @tabs = 0
  end

  def tab_up
    @tabs = @tabs + 1
  end

  def tab_down
    @tabs = @tabs - 1
  end

  def tabs
    "\t" * @tabs
  end

  def find_in_json_array(arr, key, value)
    arr.find do |e|
      e[key] == value
    end
  end

  def get_method_info(service_name, method_name)
    metadata.each do |v|
      if v[SERVICES_KEY]
        svc = find_in_json_array(v[SERVICES_KEY], NAME_KEY, "#{v[NAME_KEY]}.#{service_name}")
        if svc
          method_info = find_in_json_array(svc[FUNCTIONS_KEY], NAME_KEY, method_name)
          return method_info if method_info
        end
      end
    end
  end

  def get_message_type_and_seq(request, method_info)
    if request.has_key? ARGUMENTS_KEY
      if method_info && method_info.has_key?(ONEWAY_KEY)
        [Thrift::MessageTypes::ONEWAY, 0]
      else
        [Thrift::MessageTypes::CALL, 0]
      end
    elsif request.has_key? RESULT_KEY
      [Thrift::MessageTypes::REPLY, 1]
    elsif request.has_key? EXCEPTION_KEY
      [Thrift::MessageTypes::EXCEPTION, 1]
    else
      raise Thrift::ProtocolException.new(Thrift::ProtocolException::INVALID_DATA, "Unable to parse message type")
    end
  end

  def add(*vals)
    @params = @params.concat(vals)
  end

  def get_info(clazz)
    parts = clazz.split(".")
    program = find_in_json_array(metadata, NAME_KEY, parts[0])
    find_in_json_array(program[STRUCTS_KEY], NAME_KEY, parts[1])
  end

  def get_struct_field_list(elem_type)
    get_info(elem_type[CLASS_KEY])[FIELDS_KEY]
  end

  def raise_expected(type)
    raise Exception.new("Expected #{type}")
  end

  def parse(field_info, value, field_type_id_key, field_type_key)
    field_type = field_info[field_type_id_key]

    case field_type
    when "bool"
      raise_expected("bool") unless value == true || value == false
      add(value)
    when "i8", "i16", "i32", "i64", "double"
      raise_expected("i8") unless value.is_a? Numeric
      add(value)
    when "string"
      raise_expected("string") unless value.is_a? String
      add(value)
    when "struct", "union", "exception"
      parse_struct(get_struct_field_list(field_info[field_type_key]), value)
    when "map"
      parse_map(field_info[field_type_key], value)
    when "set", "list"
      parse_list(field_info[field_type_key], value)
    else
      raise Exception.new("Unexpected type #{field_type}")
    end
  end

  def parse_map(field_info, request)
    raise_expected("JSON Object") unless request.is_a? Hash
    key_type = string_to_type_id(field_info[KEY_TYPE_ID_KEY])
    value_type = string_to_type_id(field_info[VALUE_TYPE_ID_KEY])
    add(key_type, value_type, request.size)
    request.each do |key, value|
      parse(field_info, key, KEY_TYPE_ID_KEY, KEY_TYPE_KEY)
      parse(field_info, value, VALUE_TYPE_ID_KEY, VALUE_TYPE_KEY)
    end
  end

  def parse_list(field_info, request)
    raise_expected("JSON Array") unless request.is_a? Array
    elem_type = string_to_type_id(field_info[ELEM_TYPE_ID_KEY])
    add(elem_type, request.size)
    request.each do |value|
      parse(field_info, value, ELEM_TYPE_ID_KEY, ELEM_TYPE_KEY)
    end
  end

  def parse_struct(fields_list, request)
    raise_expected("JSON Object") unless request.is_a? Hash
    request.each do |key, value|
      field_info = find_in_json_array(fields_list, NAME_KEY, key)
      raise Exception.new("Unexpected key #{key}") unless field_info
      field_type = string_to_type_id(field_info[TYPE_ID_KEY])
      add(key, field_type, field_info[KEY_KEY] || 0)
      parse(field_info, value, TYPE_ID_KEY, TYPE_KEY)
    end
    add("", Thrift::Types::STOP, -1)
  end

  def string_to_type_id(field_type)
    case field_type
    when "bool"
      Thrift::Types::BOOL
    when "i8"
      Thrift::Types::BYTE
    when "i16"
      Thrift::Types::I16
    when "i32"
      Thrift::Types::I32
    when "i64"
      Thrift::Types::I64
    when "double"
      Thrift::Types::DOUBLE
    when "string"
      Thrift::Types::STRING
    when "struct", "union", "exception"
      Thrift::Types::STRUCT
    when "map"
      Thrift::Types::MAP
    when "set"
      Thrift::Types::SET
    when "list"
      Thrift::Types::LIST
    else
      raise Exception.new("Unknown type identifier #{field_type}")
    end
  end

  def read_message_begin
    request = JSON.parse(trans.read(nil))

    name = request[METHOD_KEY]
    method_info = get_method_info(@service, name)
    type_id, seq_id = get_message_type_and_seq(request, method_info)

    if request.has_key? ARGUMENTS_KEY
      unless method_info
        add("", Thrift::Types::STOP, -1)
        return [name, type_id, seq_id]
      end

      parse_struct(method_info[ARGUMENTS_KEY], request[ARGUMENTS_KEY])
    elsif request.has_key? RESULT_KEY
      unless method_info
        add("", Thrift::Types::STOP, -1)
        return [name, type_id, seq_id]
      end

      result = request[RESULT_KEY]
      if result.has_key? SUCCESS_KEY
        begin
          return_type = string_to_type_id(method_info[RETURN_TYPE_ID_KEY])
          add("", return_type, 0)
          parse(method_info, result[SUCCESS_KEY], RETURN_TYPE_ID_KEY, RETURN_TYPE_KEY)
          add("", Thrift::Types::STOP, -1)
        rescue Exception => e
          @err = e
        end
      elsif result.size == 0
        add("", Thrift::Types::STOP, -1)
      else
        err_name = result.keys.first
        err_info = find_in_json_array(method_info[EXCEPTIONS_KEY], NAME_KEY, err_name)
        raise Thrift::ProtocolException.new(Thrift::ProtocolException::INVALID_DATA, "Unable to parse result") unless err_info
        add(err_name, Thrift::Types::STRUCT, err_info[KEY_KEY])
        begin
          parse(err_info, result[err_name], TYPE_ID_KEY, TYPE_KEY)
        rescue Exception => e
          @err = e
        end
        add("", Thrift::Types::STOP, -1)
      end
    elsif request.has_key? EXCEPTION_KEY
      add("", Thrift::Types::STRING, 1)
      add(request[EXCEPTION_KEY][MESSAGE_KEY] || "")
      add("", Thrift::Types::I32, 2)
      add(request[EXCEPTION_KEY][TYPE_KEY] || Thrift::ApplicationException::UNKNOWN)
      add("", Thrift::Types::STOP, -1)
    else
      raise Thrift::ProtocolException.new(Thrift::ProtocolException::INVALID_DATA, "Unable to parse data")
    end
    puts request.inspect if @debug
    puts @params.inspect if @debug
    [name, type_id, seq_id]
  end

  def read_message_end
    # No-op
  end

  def read_struct_begin
    puts "#{tabs}Struct begin" if @debug
    tab_up
    raise @err if @err
  end

  def read_struct_end
    tab_down
    puts "#{tabs}Struct end -- #{@params.size}" if @debug
    # No-op
  end

  def read_field_begin
    puts "#{tabs}Field begin #{@params[0...3]}" if @debug
    tab_up unless @params[1] == Thrift::Types::STOP
    @params.slice!(0, 3)
  end

  def read_field_end
    tab_down
    puts "#{tabs}Field end" if @debug
    # No-op
  end

  def read_map_begin
    puts "#{tabs}Map begin #{@params[0...3]}" if @debug
    tab_up
    @params.slice!(0, 3)
  end

  def read_map_end
    tab_down
    puts "#{tabs}Map end" if @debug
    # No-op
  end

  def read_list_begin
    puts "#{tabs}List begin #{@params[0...2]}" if @debug
    tab_up
    @params.slice!(0, 2)
  end

  def read_list_end
    tab_down
    puts "#{tabs}List end" if @debug
    # No-op
  end

  def read_set_begin
    puts "#{tabs}Set begin #{@params[0...3]}" if @debug
    tab_up
    @params.slice!(0, 2)
  end

  def read_set_end
    tab_down
    puts "#{tabs}Set End" if @debug
    # No-op
  end

  def read_bool
    puts "#{tabs}Bool #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_byte
    puts "#{tabs}Byte #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_i16
    puts "#{tabs}i16 #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_i32
    puts "#{tabs}i32 #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_i64
    puts "#{tabs}i64 #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_double
    puts "#{tabs}Double #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_string
    puts "#{tabs}String #{@params[0]}" if @debug
    @params.slice!(0, 1).first
  end

  def read_binary
    read_string
  end

  def write_message_begin(name, type, seqid)
    @oprot.write_json_object_start
    @oprot.write_string(METHOD_KEY)
    @oprot.write_string(name.to_s)
    case type
    when Thrift::MessageTypes::CALL
      @oprot.write_string(ARGUMENTS_KEY)
    when Thrift::MessageTypes::REPLY
      @oprot.write_string(RESULT_KEY)
    when Thrift::MessageTypes::EXCEPTION
      @oprot.write_string(EXCEPTION_KEY)
    end
  end

  def write_message_end
    @oprot.write_json_object_end
  end

  def write_struct_begin(name)
    @oprot.write_json_object_start
  end

  def write_struct_end
    @oprot.write_json_object_end
  end

  def write_field_begin(name, type, id)
    @oprot.write_string(name.to_s)
  end

  def write_field_end
    # No-op
  end

  def write_field_stop
    # No-op
  end

  def write_map_begin(ktype, vtype, size)
    @oprot.write_json_object_start
  end

  def write_map_end
    @oprot.write_json_object_end
  end

  def write_list_begin(etype, size)
    @oprot.write_json_array_start
  end

  def write_list_end
    @oprot.write_json_array_end
  end

  def write_set_begin(etype, size)
    @oprot.write_json_array_start
  end

  def write_set_end
    @oprot.write_json_array_end
  end

  def write_bool(bool)
    @oprot.write_json_integer(bool)
  end

  def write_byte(byte)
    @oprot.write_byte(byte)
  end

  def write_i16(i16)
    @oprot.write_i16(i16)
  end

  def write_i32(i32)
    @oprot.write_i32(i32)
  end

  def write_i64(i64)
    @oprot.write_i64(i64)
  end

  def write_double(dub)
    @oprot.write_double(dub.to_f)
  end

  def write_string(str)
    @oprot.write_string(str)
  end

  def write_binary(buf)
    @oprot.write_binary(buf)
  end
end