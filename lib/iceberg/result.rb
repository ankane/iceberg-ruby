module Iceberg
  class Result
    include Enumerable

    attr_reader :columns, :rows

    def initialize(columns, rows)
      @columns = columns
      @rows = rows
    end

    def each
      @rows.each do |row|
        yield @columns.zip(row).to_h
      end
    end

    def empty?
      rows.empty?
    end
  end
end
