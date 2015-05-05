# More info at https://github.com/guard/guard#readme

module ::Guard
  class MakeGuard < Plugin
    def make_docs(paths=nil)
      UI.info 'Rebuilding docs...'
      out = `make docs 2>&1`
      if $?.exitstatus == 0
        UI.info 'Docs built'
      else
        UI.error '`make docs` exited with non-zero status'
        UI.debug out
        throw :task_has_failed
      end
    end

    [:run_all, :run_on_additions, :run_on_modifications, :run_on_removals].each do |method|
        alias_method method, :make_docs
    end
  end

  # Watchers should return :all to test all files, or the path of files to test
  class PyTest < Plugin
    def run_tests(test_files=[])
      test_files = test_files.select{|f| File.exists?(f)}
      UI.info "Running test files: #{test_files.join(', ')}"
      cols = ENV['COLUMNS'].to_i - 10
      out = `COLUMNS=#{cols} tox -e guard "#{test_files.join(' ')}"`
      UI.info "Test Output:\n#{out}"
    end

    def run_all(paths=nil)
      run_tests
    end
    alias_method :run_on_removals, :run_all

    def run_on_additions(paths)
      if paths == ['all']
        run_all
      else
        run_tests(paths)
      end
    end
    alias_method :run_on_modifications, :run_on_additions
  end
end

guard :pytest do
  # Test the changed file and the corresponding test
  watch(%r{^data_pipeline/(.+)\.py$}) {|m| [m[0], "tests/#{m[1]}_test.py"]  }
  watch(%r{^data_pipeline/(position_data|_kafka_producer|_position_data_builder)\.py$}) { 'tests/producer_test.py' }
  watch(%r{^tests/(.+)\_test.py$})
  watch(%r{^tests/helpers/(.+).py$}) { :all }
  watch(%r{^tests/conftest.py$}) { :all }
  watch('tox.ini') { :all }
  # rst files in root and docs directories only
  watch(%r{^([^/]+|docs/[^/]+)\.rst$})
end

guard :make_guard do
  watch(%r{^data_pipeline/(.+)\.py$})
  # rst files in root and docs directories only
  watch(%r{^([^/]+|docs/[^/]+)\.rst$})
  watch('docs/conf.py')
  watch('tox.ini')
end
