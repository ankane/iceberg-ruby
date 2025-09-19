module Iceberg
  class TableScan
    attr_reader :table

    def initialize(scan, table)
      @scan = scan
      @table = table
    end

    def plan_files
      @scan.plan_files
    end

    def snapshot
      @scan.snapshot
    end
  end
end
