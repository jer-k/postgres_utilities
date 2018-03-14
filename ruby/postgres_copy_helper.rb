module PostgresCopyHelper
  def pg_encoder
    @pg_encoder ||= PG::TextEncoder::CopyRow.new
  end

  def pg_conn
    @pg_conn ||= ActiveRecord::Base.connection.raw_connection
  end

  def insert_with_copy(model, records, timestamps=true)
    times = timestamps ? [Time.now.to_s] * 2 : []
    attribute_names = model.attribute_names - %w(id created_at updated_at)

    pg_conn.copy_data(build_copy_sql(model, attribute_names, timestamps), pg_encoder) do
      records.each do |row|
        pg_conn.put_copy_data(serialize_row(model, attribute_names, row) + times)
      end
    end
  end

  def raw_insert_with_copy(records, table_name:, columns:, schema: nil)
    qualified_table_name = conn.quote_table_name("#{schema}.#{table_name}")

    pg_conn.copy_data("COPY #{qualified_table_name} (#{columns.join(', ')}) FROM STDIN", pg_encoder) do
      records.each do |record|
        pg_conn.put_copy_data(columns.map { |column| record[column] || record[column.to_sym] })
      end
    end
  end

  def next_insert_id(model)
    query = "select last_value, is_called from #{model.table_name}_id_seq"
    sequence = ActiveRecord::Base.connection.select_one(query)
    sequence['is_called'] ? sequence['last_value'].to_i + 1 : sequence['last_value'].to_i
  end

  def copy_from_file(file, table_name, schema: nil, columns: [], copy_options: [])
    conn = ActiveRecord::Base.connection

    qualified_table_name = conn.quote_table_name("#{schema}.#{table_name}")

    quoted_columns = columns.map { |c| conn.quote_column_name(c) }
    column_list = "(#{quoted_columns.join(', ')})" if quoted_columns.present?

    options_list = "(#{copy_options.join(', ')})" if copy_options

    pg_conn.copy_data("COPY #{qualified_table_name} #{column_list} FROM STDIN #{options_list}") do
      File.open(file, 'r').each do |line|
        pg_conn.put_copy_data(line)
      end
    end
  end

  private

  def build_copy_sql(model, attribute_names, timestamps)
    timestamp_columns = timestamps ? %w(created_at updated_at) : []
    column_names = (attribute_names + timestamp_columns).join(', ')
    "COPY #{model.table_name} (#{column_names}) FROM STDIN"
  end

  def serialize_row(model, attribute_names, record)
    attribute_names.map do |attr_name|
      value = record[attr_name] || record[attr_name.to_sym] || model.columns_hash[attr_name].default
      value.is_a?(Hash) ? value.to_json : value
    end
  end
end
