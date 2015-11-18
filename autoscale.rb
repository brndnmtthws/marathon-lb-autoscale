#!/usr/bin/ruby

require 'optparse'
require 'optparse/time'
require 'optparse/uri'
require 'ostruct'
require 'pp'
require 'logger'
require 'net/http'
require 'set'
require 'json'
require 'resolv'

class Optparser
  def self.parse(args)
    options = OpenStruct.new
    options.marathon = ""
    options.haproxy = []
    options.interval = 60
    options.samples = 10
    options.cooldown = 300
    options.target_rps = 1000
    options.apps = Set.new
    options.threshold_percent = 0.25

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: autoscale.rb [options]"

      opts.separator ""
      opts.separator "Specific options:"

      opts.on("--marathon URL", URI,
              "URL for Marathon") do |value|
        options.marathon = value
      end

      opts.on("--haproxy [URLs]",
              "Comma separate list of URLs for HAProxy. If this is a Mesos-DNS A-record, " +
      "all backends will be polled.") do |value|
        options.haproxy = value.split(/,/).map{|x|URI(x)}
      end

      opts.on("--interval Float", Float, "Number of seconds (N) between update intervals " +
              "(Default: #{options.interval})") do |value|
        options.interval = value
      end

      opts.on("--samples Integer", Integer, "Number of samples to average (Default: " +
              "#{options.samples})") do |value|
        options.samples = value
      end

      opts.on("--cooldown Float", Float, "Number of additional seconds to wait after making " +
              "a scale change (Default: #{options.cooldown})") do |value|
        options.cooldown = value
      end

      opts.on("--target-rps Integer", Integer, "Target number of requests per second per " +
              "app instance (Default: #{options.target_rps})") do |value|
        options.target_rps = value
      end

      opts.on("--apps [APPS]", "Comma separated list of <app>_<service port> pairs to monitor") do |value|
        options.apps.merge(value.split(/,/))
      end

      opts.on("--threshold-percent Float", Float, "Scaling will occur when the target number " +
              "of instances differs from the actual number by at least this amount (Default: " +
              "#{options.threshold_percent})") do |value|
        options.threshold_percent = value
      end


      opts.separator ""
      opts.separator "Common options:"

      opts.on_tail("-h", "--help", "Show this message") do
        puts opts
        exit
      end
    end

    opt_parser.parse!(args)
    options
  end
end


class Autoscale
  def initialize(options)
    @options = options
    @log = Logger.new(STDOUT)
    @log.level = Logger::INFO
  end

  def run
    @log.info('Starting autoscale controller')
    @log.info("Options: #{@options.to_s}")
    @samples = 0

    @apps = {}
    @options.apps.each do |app|
      @apps[app] = {
        :req_rate => [],
        :name => app,
        :last_scaled => 0,
      }
    end

    total_samples = 0
    while true
      haproxy_data = []
      @options.haproxy.map do |haproxy|
        Resolv.getaddresses(haproxy.host).each do |host|
          uri = haproxy.clone
          uri.host = host
          haproxy_data << sample(uri)
        end
      end
      aggregate_haproxy_data(haproxy_data)

      update_current_marathon_instances

      calculate_target_instances

      if total_samples >= @options.samples
        scale_list = build_scaling_list

        scale_apps(scale_list)
      end

      total_samples += 1
      sleep @options.interval
    end
  end

  def parse_haproxy_header_labels(csv)
    header = csv.first[2..-2].split(/,/)
    # Enumerate the header
    header_labels = {}
    for i in 0..(header.length - 1)
      header_labels[i] = header[i]
    end
    header_labels
  end

  def parse_haproxy_frontends(csv, header_labels)
    csv = csv.select do |line|
      # Drop all lines which are empty or begin with # or empty
      !line.match(/^\s*#/) && !line.match(/^\s*$/)
    end
    samples = csv.map do |line|
      line.split(/,/)
    end.select do |line|
      line[1].match('FRONTEND')
    end

    frontends = {}
    samples.each do |sample|
      data = {}
      header_labels.each do |i,label|
        data[label.to_sym] = sample[i]
      end
      frontends[sample[0]] = data
    end
    frontends
  end

  def sample(haproxy)
    # Read from haproxy CSV endpoint
    csv = Net::HTTP.get(haproxy.host, haproxy.path + '/haproxy?stats;csv', haproxy.port)
    csv = csv.split(/\r?\n/)

    header_labels = parse_haproxy_header_labels(csv)
    frontends = parse_haproxy_frontends(csv, header_labels)

    # Now we've got all the frontend data sampled in `frontends`
    frontends = frontends.select do |name|
      @options.apps.include?(name)
    end

    frontends
  end

  def aggregate_haproxy_data(haproxy_data)
    @apps.each do |app,data|
      if data[:req_rate].length >= @options.samples
        data[:req_rate].rotate!
        data[:req_rate].pop
      end
      req_rate = 0
      haproxy_data.each do |d|
        req_rate += d[app][:req_rate].to_i + d[app][:qcur].to_i
      end
      data[:req_rate] << req_rate
      data[:req_rate_avg] = data[:req_rate].inject(0.0) { |sum,el| sum + el } / data[:req_rate].size
    end
  end

  def update_current_marathon_instances
    apps = Net::HTTP.get(@options.marathon.host, @options.marathon.path + '/v2/apps', @options.marathon.port)
    apps = JSON.parse(apps)
    instances = {}
    apps['apps'].each do |app|
      id = app['id'][1..-1] # trim leading '/'
      instances[id] = app['instances']
    end
    # Find our app backends
    @apps.each do |app,data|
      app_id = app.match(/(.*)_\d+$/)[1]
      if instances.has_key?(app_id)
        data[:current_instances] = instances[app_id]
      end
    end
  end

  def calculate_target_instances
    @apps.each do |app,data|
      data[:target_instances] =
        (data[:req_rate_avg] / @options.target_rps).ceil
    end
  end

  def build_scaling_list
    to_scale = {}
    @apps.each do |app,data|
      # If the target and current instances don't match, we've exceed the threshold difference, and a scale operation wasn't performed recently
      next if data[:target_instances] == data[:current_instances]
      next if (data[:target_instances] - data[:current_instances]).abs.to_f / data[:current_instances] < @options.threshold_percent
      next if data[:last_scaled] + @options.cooldown + @options.interval * @options.samples >= Time.now.to_i
      app_id = app.match(/(.*)_\d+$/)[1]
      if to_scale.has_key?(app_id) && to_scale[app_id] > data[:target_instances]
        # If another frontend requires more instances than this one, do nothing
      else
        @log.info("Scaling #{app_id} from #{data[:current_instances]} to #{data[:target_instances]} instances")
        @log.info("app_id=#{app_id} req_rate_avg=#{data[:req_rate_avg]} target_rps=#{@options.target_rps} current_rps=#{data[:req_rate_avg] / data[:current_instances]}")
        to_scale[app_id] = data[:target_instances]
        data[:last_scaled] = Time.now.to_i
      end
    end
    to_scale
  end

  def scale_apps(scale_list)
    scale_list.each do |app,instances|
      req = Net::HTTP::Put.new(@options.marathon.path + '/v2/apps/' + app, initheader = { 'Content-Type' => 'application/json'})
      req.body = JSON.generate({'instances'=>instances})
      response = Net::HTTP.new(@options.marathon.host, @options.marathon.port).start {|http| http.request(req) }
    end
  end
end

options = Optparser.parse(ARGV)
autoscale = Autoscale.new(options)
autoscale.run
