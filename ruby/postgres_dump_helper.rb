module PostgresDumpHelper
  def dump_database(folder_name=nil, dump_fmt='c', file_name:, logger: @logger)
    dump_sfx = suffix_for_format(dump_fmt)
    backup_dir = backup_directory(folder_name)
    file_path = file_name + dump_sfx
    with_config do |host, _port, db, user, pass|
      cmd = ["pg_dump",
             "--format=#{dump_fmt}",
             "--schema=vipr_latest",
             "--host=#{host}",
             "--dbname=#{db}",
             "--username=#{user}",
             '--no-owner',
             "--file=#{backup_dir}/#{file_path}"]
      env_vars = {'PGPASSWORD' => pass}

      execute_command_line_action('pg_dump', env_vars, cmd, logger)
    end
  end

  def restore_database(folder_name=nil, file_name:, logger: @logger)
    backup_dir = backup_directory(folder_name)
    file_path = backup_dir + file_name
    fmt = format_for_file(file_name)
    with_config do |host, _port, db, user, pass|
      cmd = ["pg_restore",
             "--format=#{fmt}",
             "--host=#{host}",
             "--dbname=#{db}",
             "--username=#{user}",
             "#{file_path}"]
      env_vars = {'PGPASSWORD' => pass}

      execute_command_line_action('pg_restore', env_vars, cmd, logger)
    end
  end

  def dump_table_to_csv(table_name, csv_location, logger: @logger)
    copy_command = "\\copy #{table_name} TO '#{csv_location}' ENCODING 'UTF8' CSV HEADER;"

    with_config do |host, _port, db, user, pass|
      cmd = ["psql",
             '-c',
             copy_command,
             "--host=#{host}",
             "--dbname=#{db}",
             "--username=#{user}"]
      env_vars = {'PGPASSWORD' => pass}

      execute_command_line_action('copy', env_vars, cmd, logger)
    end
  end

  def backup_directory(version_folder=nil)
    if version_folder
      backup_dir = Rails.root.join('db/', 'backups/', version_folder)
    else
      backup_dir = Rails.root.join('db/', 'backups')
    end

    FileUtils.mkdir_p(backup_dir)
    backup_dir
  end

  private

  def execute_command_line_action(action, env_vars, cmd, logger)
    Open3.popen2e(env_vars, *cmd) do |_stdin, stdouterr, thread|
      while (line = stdouterr.gets) do
        logger.info line.chomp if logger
      end

      raise "#{action} exited with an error" unless thread.value.success?
    end
  end

  def suffix_for_format(suffix)
    case suffix
      when 'c' then
        'dump'
      when 'p' then
        'sql'
      when 't' then
        'tar'
      when 'd' then
        'dir'
      else
        nil
    end
  end

  def format_for_file(file)
    case file
      when /\.dump$/ then
        'c'
      when /\.sql$/ then
        'p'
      when /\.dir$/ then
        'd'
      when /\.tar$/ then
        't'
      else
        nil
    end
  end

  def with_config
    yield ActiveRecord::Base.connection_config[:host],
      ActiveRecord::Base.connection_config[:port],
      ActiveRecord::Base.connection_config[:database],
      ActiveRecord::Base.connection_config[:username],
      ActiveRecord::Base.connection_config[:password]
  end
end
