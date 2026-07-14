module Iceberg
  class Type
  end

  class PrimitiveType < Type
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
  end

  class BinaryType < PrimitiveType
  end
end
