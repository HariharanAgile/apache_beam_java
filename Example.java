package com.dataflow.sample;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

public class Example {
    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        pipelineOptions.setRunner(DirectRunner.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);
        Schema schema = Schema.builder()
                .addStringField("match_id")
                .addStringField("season")
                .addStringField("start_date")
                .addStringField("venue")
                .addStringField("innings")
                .addStringField("ball")
                .addStringField("batting_team")
                .addStringField("bowling_team")
                .addStringField("striker")
                .addStringField("non_striker")
                .build();
        JdbcIO.DataSourceConfiguration dataSourceConfiguration = JdbcIO.DataSourceConfiguration
                .create("org.postgresql.Driver", "jdbc:postgresql://postgres:root12345@192.168.1.2:5432/temp")
                .withUsername("postgres")
                .withPassword("root12345");
        pipeline.apply(JdbcIO.<Row>read()
                .withDataSourceConfiguration(dataSourceConfiguration)
                .withQuery("SELECT * FROM ipl limit 10")
                .withRowMapper(new MyRowMapper(schema))
        ).apply(ParDo.of(new DoFn<Row, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }));
        pipeline.run().waitUntilFinish();

    }

    private static class MyRowMapper implements JdbcIO.RowMapper<Row> {
        private final Schema schema;

        public MyRowMapper(Schema schema){
            this.schema=schema;

        }
        @Override
        public Row mapRow(java.sql.ResultSet resultSet) throws Exception {
            //match_id,season,start_date,venue,innings,ball,batting_team,bowling_team,striker,non_striker
//            Schema schema = Schema.builder()
//                    .addStringField("match_id")
//                    .addStringField("season")
//                    .addStringField("start_date")
//                    .addStringField("venue")
//                    .addStringField("innings")
//                    .addStringField("ball")
//                    .addStringField("batting_team")
//                    .addStringField("bowling_team")
//                    .addStringField("striker")
//                    .addStringField("non_striker")
//                    .build();
            return Row.withSchema(this.schema)
                    .addValue(resultSet.getString("match_id"))
                    .addValue(resultSet.getString("season"))
                    .addValue(resultSet.getString("start_date"))
                    .addValue(resultSet.getString("venue"))
                    .addValue(resultSet.getString("innings"))
                    .addValue(resultSet.getString("ball"))
                    .addValue(resultSet.getString("batting_team"))
                    .addValue(resultSet.getString("bowling_team"))
                    .addValue(resultSet.getString("striker"))
                    .addValue(resultSet.getString("non_striker"))
                    .build();
        }
    }







}
