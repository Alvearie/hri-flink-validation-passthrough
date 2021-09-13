# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class HRIHelper

  def initialize(url)
    @base_url = url
    @helper = Helper.new
  end

  def hri_get_batch(tenant_id, batch_id, override_headers = {})
    url = "#{@base_url}/tenants/#{tenant_id}/batches/#{batch_id}"
    headers = { 'Accept' => 'application/json',
                'Content-Type' => 'application/json' }.merge(override_headers)
    @helper.rest_get(url, headers)
  end

  def hri_post_batch(tenant_id, request_body, override_headers = {})
    url = "#{@base_url}/tenants/#{tenant_id}/batches"
    headers = { 'Accept' => 'application/json',
                'Content-Type' => 'application/json' }.merge(override_headers)
    @helper.rest_post(url, request_body, headers)
  end

  def hri_put_batch(tenant_id, batch_id, action, additional_params = {}, override_headers = {})
    url = "#{@base_url}/tenants/#{tenant_id}/batches/#{batch_id}/action/#{action}"
    headers = { 'Accept' => 'application/json',
                'Content-Type' => 'application/json' }.merge(override_headers)
    @helper.rest_put(url, additional_params.to_json, headers)
  end

  def create_batch(tenant_id, batch_template, token)
    response = hri_post_batch(tenant_id, batch_template.to_json, {'Authorization' => "Bearer #{token}"})
    raise 'The management API failed to create a batch' unless response.code == 201
    parsed_response = JSON.parse(response.body)
    Logger.new(STDOUT).info("New Batch Created With ID: #{parsed_response['id']}")
    parsed_response['id']
  end

end