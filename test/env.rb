# (C) Copyright IBM Corp. 2021
#
# SPDX-License-Identifier: Apache-2.0

require 'rubygems'
require 'rspec'
require 'json'
require 'rspec/expectations'
require 'uri'
require 'date'
require 'json/ext'
require 'rest-client'
require 'openssl'
require 'net/http'
require 'net_http_ssl_fix'
require 'yaml'
require 'singleton'
require 'open3'
require 'csv'
require 'logger'
require 'securerandom'
require 'kafka'
require 'base64'
require 'zstd-ruby'

require 'hritesthelpers'

require_relative './spec/flink_job'
require_relative './spec/kafka_notification_builder'
require_relative './spec/kafka_record_validator'