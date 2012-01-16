module Spice
  class DataBagItem
    include Toy::Store
    include Spice::Persistence
    extend Spice::Persistence
    store :memory, {}
    endpoint "data"
    
    attribute :_id, String
    attribute :data, Hash, :default => {}    
    attribute :name, String
    
    validates_presence_of :_id, :name, :data
    
    def self.get(name, id)
      connection.data_bag_item(name, id)
    end
    
    def do_post
      attrs = data.dup
      attrs['id'] = attributes['_id']
      connection.post("/data/#{name}", attrs)
    end
    
    def do_put
      attrs = data.dup
      attrs['id'] = attributes['_id']
      connection.put("/data/#{name}/#{_id}", attrs)
    end
    
    def do_delete
      connection.delete("/data/#{name}/#{_id}")
    end
  end
end