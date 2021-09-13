# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

class COSHelper

  def initialize
    @helper = Helper.new
    @cos_url = ENV['COS_URL']
    @iam_token = IAMHelper.new.get_access_token
  end

  def upload_object_data(bucket_name, object_path, object)
    response = @helper.rest_put("#{@cos_url}/#{bucket_name}/#{object_path}", object, {'Authorization' => "Bearer #{@iam_token}"})
    raise 'Failed to upload object to COS' unless response.code == 200
  end

end