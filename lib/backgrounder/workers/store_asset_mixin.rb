# encoding: utf-8

require "open-uri"
require "aws-sdk"
require "carrierwave"
require "carrierwave/storage/fog"
require "carrierwave/storage/file"
require "carrierwave/uploader"

module CarrierWave
  module Workers

    module StoreAssetMixin
      include CarrierWave::Workers::Base

      def self.included(base)
        base.extend CarrierWave::Workers::ClassMethods
      end

      attr_reader :cache_path, :tmp_directory

      def perform(*args)
        record = super(*args)

        if record
          if (asset_tmps = record.send(:"#{column}_tmp")).is_a?(Array)
            images          = []
            tmp_directories = []
            cache_keys      = []

            asset = constantized_resource.uploaders[:"#{column}"]
            asset_tmps.each do |asset_tmp|
              store_directories(asset, asset_tmp)
              images << open(cache_path)
              tmp_directories << tmp_directory
              cache_keys << asset_tmp
            end
            record.send :"process_#{column}_upload=", true
            record.send :"#{column}_tmp=", nil
            record.send :"#{column}_processing=", false if record.respond_to?(:"#{column}_processing")
            record.send :"#{column}=", images

            if record.save!
              case cache_storage_sym
                when :fog
                  # credential_keys = CarrierWave::Uploader::Base.fog_credentials
                  # bucket_name = CarrierWave::Uploader::Base.fog_directory
                  # client = Aws::S3::Client.new(
                  #             region:            credential_keys[:region],
                  #             access_key_id:     credential_keys[:aws_access_key_id],
                  #             secret_access_key: credential_keys[:aws_secret_access_key]
                  #           )
                  # client.delete_object(bucket: bucket_name, key: tmp_directory)
                when :file
                  tmp_directories.each {|d| FileUtils.rm_r(d, :force => true)}
                when :postgresql_lo
                  tmp_directories.each {|d| FileUtils.rm_r(d, :force => true)}
                  cache_keys.each {|cc| CarrierWave::Storage::PostgresqlLo::CacheFile.new.delete(cc)}
              end
            end

          else
            cache_key = record.send(:"#{column}_tmp")
            store_directories(record.send(:"#{column}"),
                              record.send(:"#{column}_tmp"))
            record.send :"process_#{column}_upload=", true
            record.send :"#{column}_tmp=", nil
            record.send :"#{column}_processing=", false if record.respond_to?(:"#{column}_processing")
            open(cache_path) do |f|
              record.send :"#{column}=", f
            end

            if record.save!
              case cache_storage_sym
                when :fog
                  # credential_keys = CarrierWave::Uploader::Base.fog_credentials
                  # bucket_name = CarrierWave::Uploader::Base.fog_directory
                  # client = Aws::S3::Client.new(
                  #             region:            credential_keys[:region],
                  #             access_key_id:     credential_keys[:aws_access_key_id],
                  #             secret_access_key: credential_keys[:aws_secret_access_key]
                  #           )
                  # client.delete_object(bucket: bucket_name, key: tmp_directory)
                when :file
                  FileUtils.rm_r(tmp_directory, :force => true)
                when :postgresql_lo
                  FileUtils.rm_r(tmp_directory, :force => true)
                  CarrierWave::Storage::PostgresqlLo::CacheFile.new.delete(cache_key)
              end
            end
          end
        else
          when_not_ready
        end
      end

      private

      def cache_storage_sym
        cache_storage =  CarrierWave::Uploader::Base.cache_storage.name
        storage_engines = CarrierWave::Uploader::Base.storage_engines
        name = storage_engines.invert[cache_storage]
        fail("Unknown cache storage engine #{cache_storage}") unless name
        name
      end

      def store_directories(asset, asset_tmp)
        case cache_storage_sym
          when :fog
            cache_url = "http://#{CarrierWave::Uploader::Base.fog_directory}.s3.amazonaws.com"
            @cache_path = "#{cache_url}/#{asset.cache_dir}/#{asset_tmp}"
            @tmp_directory = "#{asset.cache_dir}/#{asset_tmp}"

          when :file
            cache_directory  = File.expand_path(asset.cache_dir, asset.root.is_a?(Proc) ? asset.root.call : asset.root)
            @cache_path      = File.join(cache_directory, asset_tmp)
            @tmp_directory   = File.join(cache_directory, asset_tmp.split("/").first)

          when :postgresql_lo
            cache_directory  = File.expand_path(asset.cache_dir, asset.root.is_a?(Proc) ? asset.root.call : asset.root)
            @cache_path      = File.join(cache_directory, asset_tmp)
            @tmp_directory   = File.join(cache_directory, asset_tmp.split("/").first)

            cf = CarrierWave::Storage::PostgresqlLo::CacheFile.new
            FileUtils.mkdir_p(@tmp_directory)
            content = cf.read(asset_tmp)
            ::File.open(@cache_path, 'wb') {|io| io.write(content)}
        end
      end

    end # StoreAssetMixin

  end # Workers
end # Backgrounder
