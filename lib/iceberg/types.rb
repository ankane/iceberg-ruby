module Iceberg
  class BooleanType
  end

  class IntType
  end

  class LongType
  end

  class FloatType
  end

  class DoubleType
  end

  class DecimalType
    attr_reader :precision, :scale

    def initialize(precision, scale)
      @precision = precision
      @scale = scale
    end
  end

  class DateType
  end

  class TimeType
  end

  class TimestampType
  end

  class TimestamptzType
  end

  class TimestampNanoType
  end

  class TimestamptzNanoType
  end

  class StringType
  end

  class UUIDType
  end

  class FixedType
    attr_reader :length

    def initialize(length)
      @length = length
    end
  end

  class BinaryType
  end
end
