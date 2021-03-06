package com.phone.etl.mr.etl;

import com.phone.etl.common.EventLogsConstant;
import com.phone.etl.ip.LogUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;

public class EtlToHdfsMapper extends Mapper<LongWritable, Text, LogWritable, NullWritable> {

    static Logger logger = Logger.getLogger(EtlToHdfsMapper.class);


        static LogWritable k = new LogWritable();
        static int inputRecords,filterRecords,outputRecords = 0;

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                inputRecords ++;
                String line = value.toString();
                if(StringUtils.isEmpty(line)){
                    filterRecords++;
                    return;
                }

                //此处对 ip ，userAgent，params进行解析
                Map<String, String> map = LogUtil.parserLog(line);

                //获取事件的别名
                String eventName = map.get(EventLogsConstant.EVENT_COLUMN_NAME_EVENT_NAME);
                EventLogsConstant.EventEnum event = EventLogsConstant.EventEnum.valueOfAlias(eventName);

                switch (event){
                    case LAUNCH:
                    case EVENT:
                    case PAGEVIEW:
                    case CHARGEREQUEST:
                    case CHARGESUCCESS:
                    case CHARGEREFUND:
                        hadleLog(map,context);
                        break;
                    default:
                        break;

                    }

        }

        private void hadleLog(Map<String, String> map, Context context) {
            try {
                //map循环
                for(Map.Entry<String,String> en : map.entrySet()){
//                    this.k.setB_iev(en.getValue());
                    switch (en.getKey()){
                        case "ver": this.k.setIp(en.getValue()); break;
                        case "s_time": this.k.setS_time(en.getValue()); break;
                        case "en": this.k.setEn(en.getValue()); break;
                        case "u_ud": this.k.setU_uid(en.getValue()); break;
                        case "u_mid": this.k.setU_mid(en.getValue()); break;
                        case "u_sd": this.k.setU_sd(en.getValue()); break;
                        case "c_time": this.k.setC_time(en.getValue()); break;
                        case "l": this.k.setL(en.getValue()); break;
                        case "b_iev": this.k.setB_iev(en.getValue()); break;
                        case "b_rst": this.k.setB_rst(en.getValue()); break;
                        case "p_url": this.k.setP_url(en.getValue()); break;
                        case "p_ref": this.k.setP_ref(en.getValue()); break;
                        case "tt": this.k.setTt(en.getValue()); break;
                        case "pl": this.k.setPl(en.getValue()); break;
                        case "ip": this.k.setIp(en.getValue()); break;
                        case "oid": this.k.setOid(en.getValue()); break;
                        case "on": this.k.setOn(en.getValue()); break;
                        case "cua": this.k.setCua(en.getValue()); break;
                        case "cut": this.k.setCut(en.getValue()); break;
                        case "pt": this.k.setPt(en.getValue()); break;
                        case "ca": this.k.setCa(en.getValue()); break;
                        case "ac": this.k.setAc(en.getValue()); break;
                        case "kv_": this.k.setKv_(en.getValue()); break;
                        case "du": this.k.setDu(en.getValue()); break;
                        case "browserName": this.k.setBrowserName(en.getValue()); break;
                        case "browserVersion": this.k.setBrowserVersion(en.getValue()); break;
                        case "osName": this.k.setOsName(en.getValue()); break;
                        case "osVersion": this.k.setOsVersion(en.getValue()); break;
                        case "country": this.k.setCountry(en.getValue()); break;
                        case "province": this.k.setProvince(en.getValue()); break;
                        case "city": this.k.setCity(en.getValue()); break;
                    }
                }
                this.outputRecords ++;
                context.write(k,NullWritable.get());
            } catch (Exception e) {
                logger.warn("etl最终输出异常",e);
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            logger.info("inputRecords:"+this.inputRecords+"  filterRecords:"+filterRecords+"  outputRecords:"+outputRecords);
        }
    }

