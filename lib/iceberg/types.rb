module Iceberg
  class Type
    def inspect
      "#<#{self.class.name}>"
    end
  end

  class PrimitiveType < Type
    def ==(other)
      other.is_a?(self.class)
    end
  end

  class BooleanType < PrimitiveType
  end

  class IntType < PrimitiveType
  end

  class LongType < PrimitiveType
  end

  class FloatType < PrimitiveType
  end

  class DoubleType < PrimitiveType
  end

  class DecimalType < PrimitiveType
    attr_reader :precision, :scale

    def initialize(precision, scale)
      @precision = precision
      @scale = scale
    end

    def ==(other)
      other.is_a?(self.class) && other.precision == @precision && other.scale == @scale
    end

    def inspect
      "#<#{self.class.name} precision=#{@precision.inspect}, scale=#{@scale.inspect}>"
    end
  end

  class DateType < PrimitiveType
  end

  class TimeType < PrimitiveType
  end

  class TimestampType < PrimitiveType
  end

  class TimestamptzType < PrimitiveType
  end

  class TimestampNanoType < PrimitiveType
  end

  class TimestamptzNanoType < PrimitiveType
  end

  class StringType < PrimitiveType
  end

  class UUIDType < PrimitiveType
  end

  class FixedType < PrimitiveType
    attr_reader :length

    def initialize(length)
      @length = length
    end

    def ==(other)
      other.is_a?(self.class) && other.length == @length
    end

    def inspect
      "#<#{self.class.name} length=#{@length.inspect}>"
    end
  end

  class BinaryType < PrimitiveType
  end

  class StructType < Type
    attr_reader :fields

    def initialize(*fields)
      @fields = fields
    end

    def ==(other)
      other.is_a?(self.class) && other.fields == @fields
    end

    def inspect
      "#<#{self.class.name} fields=#{@fields.inspect}>"
    end
  end

  class ListType < Type
    attr_reader :element_field

    def initialize(element_field)
      @element_field = element_field
    end

    def ==(other)
      other.is_a?(self.class) && other.element_field == @element_field
    end

    def inspect
      "#<#{self.class.name} element_field=#{@element_field.inspect}>"
    end
  end

  class MapType < Type
    attr_reader :key_field, :value_field

    def initialize(key_field, value_field)
      @key_field = key_field
      @value_field = value_field
    end

    def ==(other)
      other.is_a?(self.class) && other.key_field == @key_field && other.value_field == @value_field
    end

    def inspect
      "#<#{self.class.name} key_field=#{@key_field.inspect}, value_field=#{@value_field.inspect}>"
    end
  end
end
