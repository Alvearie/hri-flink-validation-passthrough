#!/usr/bin/env ruby

# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require_relative '../env'

# This script uploads JUnit test reports to Cloud Object Storage to be used by the Allure application to generate HTML
# test trend reports for the IVT and Nightly tests. More information on Allure can be found here: https://github.com/allure-framework/allure2
#
# The 'ivttest.xml' and 'nightlytest.xml' JUnit reports are uploaded to the 'wh-hri-dev1-allure-reports' Cloud Object Storage bucket,
# which is also mounted on the 'allure' kubernetes pod. This bucket keeps 30 days of reports that will be used to generate a
# historical HTML report when the allure executable is invoked on the pod.

cos_helper = HRITestHelpers::COSHelper.new(ENV['COS_URL'], ENV['IAM_CLOUD_URL'], ENV['CLOUD_API_KEY'])
logger = Logger.new(STDOUT)
time = Time.now.strftime '%Y%m%d%H%M%S'

if ARGV[0] == 'IVT'
  logger.info("Uploading ivttest-#{time}.xml to COS")
  doc = Nokogiri::XML(File.open("#{Dir.pwd}/ivttest.xml")) { |file| file.noblanks }
  doc.search('//testsuite').attribute('name').value = "hri-flink-validation-passthrough - #{ENV['BRANCH_NAME']} - IVT"
  File.rename("#{Dir.pwd}/ivttest.xml", "#{Dir.pwd}/hri-flink-validation-passthrough-ivttest-#{time}.xml")
  cos_helper.upload_object_data('wh-hri-dev1-allure-reports', "hri-flink-validation-passthrough-ivttest-#{time}.xml", File.read(File.join(Dir.pwd, "hri-flink-validation-passthrough-ivttest-#{time}.xml")))
elsif ARGV[0] == 'Nightly'
  logger.info("Uploading nightlytest-#{time}.xml to COS")
  doc = Nokogiri::XML(File.open("#{Dir.pwd}/nightlytest.xml")) { |file| file.noblanks }
  doc.search('//testsuite').attribute('name').value = "hri-flink-validation-passthrough - #{ENV['BRANCH_NAME']} - Nightly"
  File.rename("#{Dir.pwd}/nightlytest.xml", "#{Dir.pwd}/hri-flink-validation-passthrough-nightlytest-#{time}.xml")
  cos_helper.upload_object_data('wh-hri-dev1-allure-reports', "hri-flink-validation-passthrough-nightlytest-#{time}.xml", File.read(File.join(Dir.pwd, "hri-flink-validation-passthrough-nightlytest-#{time}.xml")))
else
  raise "Invalid argument: #{ARGV[0]}. Valid arguments: 'IVT' or 'Nightly'"
end