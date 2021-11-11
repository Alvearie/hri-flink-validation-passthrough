#!/usr/bin/env ruby

# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require_relative '../env'

# This script uploads JUnit test reports to Cloud Object Storage to be used by the Allure application to generate HTML
# test trend reports for the IVT and Nightly tests. More information on Allure can be found here: https://github.com/allure-framework/allure2
#
# The 'ivttest.xml' and 'nightlytest.xml' JUnit reports are uploaded to the 'wh-hri-dev1-allure-reports' Cloud Object Storage bucket,
# which is also mounted on the 'allure' kubernetes pod. This bucket keeps the latest test report that will be used to generate a
# # test dashboard when the allure executable is invoked on the pod.

cos_helper = HRITestHelpers::COSHelper.new(ENV['COS_URL'], ENV['IAM_CLOUD_URL'], ENV['CLOUD_API_KEY'])
logger = Logger.new(STDOUT)

if ARGV[0] == 'IVT'
  logger.info("Uploading ivttest-#{ENV['BRANCH_NAME']}.xml to COS")
  unless File.exists?("#{Dir.pwd}/ivttest.xml")
    logger.info('ivttest.xml not found. Skipping test report upload.')
    exit 0
  end

  doc = Nokogiri::XML(File.open("#{Dir.pwd}/ivttest.xml")) { |file| file.noblanks }
  doc.search('//testsuite').attribute('name').value = "hri-flink-validation-passthrough - #{ENV['BRANCH_NAME']} - IVT"
  File.write("#{Dir.pwd}/ivttest.xml", doc)
  File.rename("#{Dir.pwd}/ivttest.xml", "#{Dir.pwd}/hri-flink-validation-passthrough-ivttest-#{ENV['BRANCH_NAME']}.xml")
  cos_helper.upload_object_data('wh-hri-dev1-allure-reports', "hri-flink-validation-passthrough-ivttest-#{ENV['BRANCH_NAME']}.xml", File.read(File.join(Dir.pwd, "hri-flink-validation-passthrough-ivttest-#{ENV['BRANCH_NAME']}.xml")))
elsif ARGV[0] == 'Nightly'
  logger.info("Uploading nightlytest-#{ENV['BRANCH_NAME']}.xml to COS")
  unless File.exists?("#{Dir.pwd}/nightlytest.xml")
    logger.info('nightlytest.xml not found. Skipping test report upload.')
    exit 0
  end

  doc = Nokogiri::XML(File.open("#{Dir.pwd}/nightlytest.xml")) { |file| file.noblanks }
  doc.search('//testsuite').attribute('name').value = "hri-flink-validation-passthrough - #{ENV['BRANCH_NAME']} - Nightly"
  File.write("#{Dir.pwd}/nightlytest.xml", doc)
  File.rename("#{Dir.pwd}/nightlytest.xml", "#{Dir.pwd}/hri-flink-validation-passthrough-nightlytest-#{ENV['BRANCH_NAME']}.xml")
  cos_helper.upload_object_data('wh-hri-dev1-allure-reports', "hri-flink-validation-passthrough-nightlytest-#{ENV['BRANCH_NAME']}.xml", File.read(File.join(Dir.pwd, "hri-flink-validation-passthrough-nightlytest-#{ENV['BRANCH_NAME']}.xml")))
else
  raise "Invalid argument: #{ARGV[0]}. Valid arguments: 'IVT' or 'Nightly'"
end