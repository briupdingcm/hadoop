#!/bin/bash
package=com.briup.rmc.coMatrix
out_dir=$3
date
#计算物品间共生关系
yarn jar target/bd1602rmc-0.0.1.jar ${package}.ItemCooccurence -D input=$1 -D output=${out_dir}/cooc -D mapreduce.job.reduces=$2
#计算物品间共现矩阵
yarn jar target/bd1602rmc-0.0.1.jar ${package}.ItemCooccurenceMatrix -D input=${out_dir}/cooc -D output=${out_dir}/matrix -D mapreduce.job.reduces=$2
#将共现矩阵转换成每个商品的共现向量
yarn jar target/bd1602rmc-0.0.1.jar ${package}.ItemCooccurenceList -D input=${out_dir}/matrix -D output=${out_dir}/matrixList -D mapreduce.job.reduces=$2
#物品被用户偏好的向量
yarn jar target/bd1602rmc-0.0.1.jar ${package}.ItemPreferenceList -D input=$1 -D output=${out_dir}/itemList -D mapreduce.job.reduces=$2
#上述两个向量的乘
yarn jar target/bd1602rmc-0.0.1.jar ${package}.MatrixVectorMultiple -D user=${out_dir}/itemList -D item=${out_dir}/matrixList  -D output=${out_dir}/userList -D mapreduce.job.reduces=$2
#以用户为中心，累加其对物品的偏好向量
yarn jar target/bd1602rmc-0.0.1.jar ${package}.RecommendVector -D input=${out_dir}/userList -D output=${out_dir}/vector -D mapreduce.job.reduces=$2
#去除用户曾经关联的物品
yarn jar target/bd1602rmc-0.0.1.jar ${package}.CheckDriver -D input=$1 -D vector=${out_dir}/vector -D output=${out_dir}/output -D mapreduce.job.reduces=$2
#对一个用户未关联商品按计算偏好度排序，形成推荐结果
#yarn jar target/bd1602rmc-0.0.1.jar ${package}.ResultSort -D input=${out_dir}/output -D recommendNum=10  -D mapreduce.job.reduces=$2

date
