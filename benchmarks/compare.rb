# frozen_string_literal: true

require "json"

unless ARGV.size == 2
  abort "Usage: ruby benchmarks/compare.rb postgres.json mongodb.json"
end

postgres, mongodb = ARGV.map { |path| JSON.parse(File.read(path)) }
expected = %w[jobs workers payload_bytes repetitions]
expected.each do |key|
  abort "Mismatched benchmark #{key}: workloads must be identical" unless postgres.fetch(key) == mongodb.fetch(key)
end
abort "First result must be active_record" unless postgres.fetch("backend") == "active_record"
abort "Second result must be mongodb" unless mongodb.fetch("backend") == "mongodb"

def median(values)
  sorted = values.sort
  middle = sorted.length / 2
  sorted.length.odd? ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2.0
end

sql_samples = postgres.fetch("samples").group_by { |sample| sample.fetch("scenario") }
mongo_samples = mongodb.fetch("samples").group_by { |sample| sample.fetch("scenario") }
abort "Mismatched benchmark scenarios" unless sql_samples.keys.sort == mongo_samples.keys.sort

comparisons = sql_samples.map do |scenario, baseline|
  candidate = mongo_samples.fetch(scenario)
  sql_throughput = median(baseline.map { |sample| sample.fetch("jobs_per_second") })
  mongo_throughput = median(candidate.map { |sample| sample.fetch("jobs_per_second") })
  row = {
    scenario: scenario,
    postgres_jobs_per_second: sql_throughput,
    mongodb_jobs_per_second: mongo_throughput,
    throughput_ratio: mongo_throughput / sql_throughput,
    throughput_pass: mongo_throughput >= sql_throughput,
    mongodb_transaction_retries: candidate.sum { |sample| sample.fetch("transaction_retries", 0) }
  }
  if baseline.first.key?("p99_claim_ms")
    sql_p99 = median(baseline.map { |sample| sample.fetch("p99_claim_ms") })
    mongo_p99 = median(candidate.map { |sample| sample.fetch("p99_claim_ms") })
    row.merge!(postgres_p99_ms: sql_p99, mongodb_p99_ms: mongo_p99,
      p99_ratio: mongo_p99 / sql_p99, p99_pass: mongo_p99 <= sql_p99)
  end
  row
end

passed = comparisons.all? { |row| row.fetch(:throughput_pass) && row.fetch(:p99_pass, true) }
puts JSON.pretty_generate(gate: "MongoDB throughput >= PostgreSQL; MongoDB p99 claim latency <= PostgreSQL",
  passed: passed, comparisons: comparisons)
exit(passed ? 0 : 1)
