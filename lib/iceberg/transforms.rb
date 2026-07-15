module Iceberg
  class Transform
  end

  class IdentityTransform < Transform
  end

  class BucketTransform < Transform
    attr_reader :num_buckets

    def initialize(num_buckets)
      @num_buckets = num_buckets
    end
  end

  class TruncateTransform < Transform
    attr_reader :width

    def initialize(width)
      @width = width
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
