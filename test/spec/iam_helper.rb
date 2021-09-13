# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class IAMHelper

  def initialize
    @helper = Helper.new
    @iam_cloud_url = ENV['IAM_CLOUD_URL']
  end

  def get_access_token
    response = @helper.rest_post("#{@iam_cloud_url}/identity/token", { 'grant_type' => 'urn:ibm:params:oauth:grant-type:apikey', 'apikey' => ENV['CLOUD_API_KEY'] }, { 'Content-Type' => 'application/x-www-form-urlencoded', 'Accept' => 'application/json' })
    raise 'IAM token request failed' unless response.code == 200
    JSON.parse(response.body)['access_token']
  end

end