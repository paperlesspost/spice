require 'cgi'

module Spice
  class Connection
    module Search
      # @option options [String] :q The Solr search query string
      # @option options [String] :sort Order by which to sort the results
      # @option options [Numeric] :start The number by which to offset the results
      # @option options [Numeric] :rows The maximum number of rows to return
      def search(index, options=Mash.new)
        index = index.to_s
        options = {:q => options} if options.is_a? String
        options.symbolize_keys!

        options[:q] ||= '*:*'
        options[:sort] ||= "X_CHEF_id_CHEF_X asc"
        options[:start] ||= 0
        options[:rows] ||= 1000

        # clean up options hash
        options.delete_if{|k,v| !%w(q sort start rows).include?(k.to_s)}

        params = options.collect{ |k, v| "#{k}=#{CGI::escape(v.to_s)}"}.join("&")
        case index
        when 'node'
          search_and_map(index, params) do |node|
            Spice::Node.get_or_new(node)
          end
        when 'role'
          search_and_map(index, params) do |role|
            Spice::Role.get_or_new(role)
          end
        when 'client'
          search_and_map(index, params) do |client|
            Spice::Client.get_or_new(client)
          end
        when 'environment'
          search_and_map(index, params) do |env|
            env['attrs'] = env.delete('attributes')
            Spice::Environment.get_or_new(env)
          end
        else
          # assume it's a data bag
          search_and_map(index, params) do |db|
            data = db['raw_data']
            Spice::DataBagItem.get_or_new(data)
          end
        end
      end # def search

      private
      def search_and_map(index, params, &block)
        url = "/search/#{CGI::escape(index.to_s)}?#{params}"
        result = get(url)
        if result && result['rows']
          result['rows'].map(&block)
        else
          raise Spice::Error.new(RuntimeError.new("Error making search to '#{url}'.\nResponse:\n#{result}"))
        end
      end

    end # module Search
  end # class Connection
end # module Spice
