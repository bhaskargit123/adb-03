# Databricks notebook source
dfcat=spark.read.format('parquet').load('/mnt/source/category/')
