# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class ElasticHelper

  def initialize
    @helper = Helper.new
    @elastic_url = ENV['ELASTIC_URL']
    @headers = { 'Content-Type': 'application/json' }
    @basic_auth = { user: ENV['ELASTIC_USER'], password: ENV['ELASTIC_PASSWORD'], ssl_ca_file: File.join(File.dirname(__FILE__), "../../elastic.crt") }
  end

  def es_delete_by_query(index, query)
    @helper.rest_post("#{@elastic_url}/#{index}-batches/_delete_by_query?q=#{query}", nil, @headers, @basic_auth)
  end

end