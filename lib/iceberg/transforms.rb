module Iceberg
  class Transform
    def ==(other)
      other.is_a?(self.class)
    end

    def inspect
      "#<#{self.class.name}>"
    end
  end

  class IdentityTransform < Transform
  end

  class BucketTransform < Transform
    attr_reader :num_buckets

    def initialize(num_buckets)
      @num_buckets = num_buckets
    end

    def ==(other)
      other.is_a?(self.class) && other.num_buckets == @num_buckets
    end

    def inspect
      "#<#{self.class.name} num_buckets=#{@num_buckets.inspect}>"
    end
  end

  class TruncateTransform < Transform
    attr_reader :width

    def initialize(width)
      @width = width
    end

    def ==(other)
      other.is_a?(self.class) && other.width == @width
    end

    def inspect
      "#<#{self.class.name} width=#{@width.inspect}>"
    end
  end

  class YearTransform < Transform
  end

  class MonthTransform < Transform
  end

  class DayTransform < Transform
  end

  class HourTransform < Transform
  end

  class VoidTransform < Transform
  end

  class UnknownTransform < Transform
  end
end
