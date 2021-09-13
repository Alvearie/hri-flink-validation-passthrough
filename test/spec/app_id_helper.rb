# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class AppIDHelper

  def initialize
    @helper = Helper.new
    @app_id_url = ENV['OIDC_ISSUER']
  end

  def get_access_token(app_id_credentials, scopes, audience)
    response = @helper.rest_post("#{@app_id_url}/token", {'grant_type' => 'client_credentials', 'scope' => scopes, 'audience' => audience}, {'Content-Type' => 'application/x-www-form-urlencoded', 'Accept' => 'application/json', 'Authorization' => "Basic #{app_id_credentials}"})
    raise 'App ID token request failed' unless response.code == 200
    JSON.parse(response.body)['access_token']
  end

end