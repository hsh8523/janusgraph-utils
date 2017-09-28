package com.ibm.janusgraph.utils.importer;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * Created by shaohuahe on 2017/9/13.
 */
public class ConvertTicketData {
    private static HashMap<String, String> titles = new HashMap<>();
    private static HashMap<String, String> files = new HashMap<>();

    private static HashMap<String, Integer> startNums = new HashMap<>();
    private static HashMap<String, Integer> incNums = new HashMap<>();

    static {
        titles.put("center", "node_id,ticketid,syscode,officecode,continueflag,firstticketid,status,passengername,passengerid,salescode,sellcode,printernum,reserveddate,tickettype,settlecode,ticketnum,pnr,bookingdate,ticketcurrency,ticketprice,ticketformula,cashcurrency,cashamount,totalcurrency,totalprice,countrycode,commissionrate,skywayid,paymentid,clientsettlecode,income,expense,discount,invoicenum,clientid,checknum,settleunitid,groupid,insurance,iffetch,payoff,audit,lastmdateofstatus,lastdateofstatus,lastscodeofstatus,lastmdateofaudit,lastdateofaudit,lastscodeofaudit,lastmdateofpay,lastdateofpay,lastscodeofpay,ticketdetailid,tourcode,returnnum,fee_a,fee_c,memo,cardtype,cardnum,phonetype,phonenum,creditnum,ticket_dori,clid,flightstatus,taxamount1,taxamount2,taxamount3,zval,usestatus,eticketflag,upbuild,active,operatetime,cpid,counterstatus,receiptnum,departmentcode,settleaccountsnum,ticketsendercode,purchasedcode,bspsettledate,memo2,traveltype,incomepayed,ticketnum_ex,backmoney,isexchange,yqfee,yrfee,ticketyszk,fix_comms_amt,ticketfullnum,rfnd_flag,uncouponflag");
        titles.put("flight", "node_id,ticketid,flights,engflights,cities,classes,flightnums,serialno,ticketnum,takeoffdate,returnflight,flightflag,stopover,departuretime,freebaggageallowance,bookingstatus,aircrafttype,classofservice,farebsis,couponstatus,indicator,aircorpcode,airflightcode");
        titles.put("detail", "node_id,tkt_id,tkt_rcd_dt,tkt_fmt_cd,tkt_nbr,tkt_airln_nbr,rmk_auto_acct_ind,rmk_bkg_offc_nbr,rmk_tkg_agt_nbr,rmk_bkg_agt_nbr,tkg_offc_cd,tkt_txn_cd,conjct_tkt_ind,conjct_tkt_nbr,conjct_tkt_rltv,conjct_tkt_tot_qty,cpn_usg_typ,asct_tkt_nbr,pnr_ref,tkt_iss_dt,rmk_bkg_dt,tkg_agt_nbr,flt_statis_cd,prc_ind,et_ind,tru_orgn_dest,tour_cd,tkt_fr_crncy_cd,tkt_amt,tkt_crncy_cd,tkt_crncy_cd_orig,eqt_fr_paid_amt,eqt_fr_paid_crncy_cd,eqt_fr_paid_crncy_cd_orig,comms_rt,comms_amt,nr_ind,tax_cd_1,tax_amt_1,tax_cd_2,tax_amt_2,tax_cd_3,tax_amt_3,cp_tax_cd,cp_tax_amt,dpt_airpt_cd_1,arrv_airpt_cd_1,airln_cd_1,flt_nbr_1,dpt_dt_1,dpt_airpt_cd_2,arrv_airpt_cd_2,airln_cd_2,flt_nbr_2,dpt_dt_2,dpt_airpt_cd_3,arrv_airpt_cd_3,airln_cd_3,flt_nbr_3,dpt_dt_3,dpt_airpt_cd_4,arrv_airpt_cd_4,airln_cd_4,flt_nbr_4,dpt_dt_4,flt_bkg_stat_1,seg_fr_bs_1,tkg_sub_cls_cd_all,frn_seg_cnt,seg_cnt,rq_seg_cnt,pmt_typ_cd_1,pmt_typ_frm_1,pmt_acct_nbr_1,pmt_info_1,pmt_info_2,sale_typ,endr_restrc,zvalue,pax_typ_cd,rmk_grp_ind,dev_nbr,iss_tkt_pid,seg_gross_amt,fr_amt,z_comms_rt,z_comms_amt,bank_booking_id,b2b_sales_id,vip_nbr,cpn_1_oth_pnr_ref,bop_ind,gp_ind,pay_bank,sale_channel,yeexing_ind,xinga_ind,xingji_ind,shengyi_ind,smallsun_ind,cpn1_state,cpn2_state,cpn3_state,cpn4_state,cpn_current_state,exng_tkt_nbr,exng_tkt_nbr_final,tkt_use_state,apply_id,booking_id,drawer_id,b2b_update_status,data_sources,exchange_update_status,ddd");
        titles.put("usage", "node_id,tkt_rcd_dt,bsp_id,tkt_nbr,tkt_airln_nbr,flt_statis_cd,tkt_cpn_id,et_stat_cd,lcd_dpt_dt,carr_cd,flt_nbr,flt_nbr_sfx,dpt_airpt_cd,arrv_airpt_cd,grp_ind,exng_tkt_nbr,tkt_iss_dt,tkg_agt_nbr,fst_tkt_nbr,fst_tkt_id,cdshr_airln_cd,cdshr_flt_nbr,flt_dpt_tm,flt_arrv_tm,seg_fr_bs,pax_typ_cd,tkt_bcv,rmk_tkg_offc_nbr,rmk_bkg_offc_nbr,pmt_typ_cd_1,tkt_crncy_cd,eqt_fr_paid_amt,tkt_amt,total_tax,cp_tax_cd,cp_tax_amt,seg_gross_amt,seg_tax_amt,seg_net_amt,seg_disc_amt,seg_comms_amt,lcd_dpt_dt_ecp_id,tkt_ecp_id_a,tkt_ecp_id_b,table_join_type_a,table_join_type_b,tk_sub_cls_cd,extendair_ind");

        files.put("ds_tkt_tktcenter.txt", "center");
        files.put("ds_tkt_ticketflights1.txt", "flight");
        files.put("ds_agtpd_tkt_detail_all.txt", "detail");
        files.put("ds_agtpd_tkt_usage_all.txt", "usage");

        startNums.put("center", 5);
        startNums.put("flight", 5);
        startNums.put("detail", 1);
        startNums.put("usage", 1);

        incNums.put("center", 5);
        incNums.put("flight", 5);
        incNums.put("detail", 1);
        incNums.put("usage", 1);

    }


    public static void main(String args[]) throws Exception {
        long t1 = System.currentTimeMillis();
        if (null == args || args.length < 3) {
            System.err.println("Usage: cdata <directory>  startNum totalNum");
            System.exit(1);
        }

        File file = new File(args[0]);
        if (!file.exists() || !file.isDirectory()) {
            System.err.println("directory not exist");
            System.exit(1);
        }

        // 增加title,id
        files.forEach((s, s2) -> {
            String readFileName = args[0] + File.separator + s;
            String writeFileName = args[0] + File.separator + s2 + ".csv";
            String title = titles.get(s2);
            int startNum = startNums.get(s2) + Integer.parseInt(args[1]);
            int incNum = incNums.get(s2);
            ConvertTicketData.readWriteFileByLines(readFileName, writeFileName, title, s2, startNum, incNum);
        });

        System.out.printf("完成数据增加title操作");
        //生产edge

        int count = Integer.parseInt(args[1]) + Integer.parseInt(args[2]);
        int startNum = startNums.get("center") + Integer.parseInt(args[1]);
        int incNum = incNums.get("center");
        ConvertTicketData.generateEdge(args[0] + File.separator + "center_to_flight.csv", count, "center", "flight", startNum, incNum);

        ConvertTicketData.generateEdge(args[0] + File.separator + "center_to_detail.csv", count, "center", "detail",startNum, incNum);

        startNum = startNums.get("detail") + Integer.parseInt(args[1]);
        incNum = incNums.get("detail");
        ConvertTicketData.generateEdge(args[0] + File.separator + "detail_to_usage.csv",count, "detail", "usage", startNum, incNum);
        System.out.printf("完成生产点变边关系数据");
    }


    public static void generateEdge(String writeFileName, int count, String leftPrifix, String rightPrifix, int startNum, int incNum) {
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(new File(writeFileName)));
            writer.write("Left,Right");
            // 一次读入一行，直到读入null为文件结束
            int line = 0;
            while (startNum <= count) {
                // 显示行号
                writer.newLine();
                writer.write(leftPrifix + startNum + "," + rightPrifix + startNum);
                startNum = startNum + incNum;
                line++;
                if (line % 1000 == 0) {
                    System.out.println(writeFileName + ":" + line);
                    writer.flush();
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e1) {
                }
            }
        }
    }


    public static void readWriteFileByLines(String readFileName, String writeFileName, String title, String prifix, int startNum, int incNum) {
        BufferedReader reader = null;
        BufferedWriter writer = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            reader = new BufferedReader(new FileReader(new File(readFileName)));
            writer = new BufferedWriter(new FileWriter(new File(writeFileName)));
            writer.write(title);
            String tempString = null;
            int line = 1;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                // 显示行号
                writer.newLine();
                writer.write(prifix + startNum + "," + tempString);
                line++;
                startNum = startNum + incNum;
                if (line % 1000 == 0) {
                    System.out.println(prifix + ":" + line);
                    writer.flush();
                }
            }

            reader.close();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }

            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e1) {
                }
            }
        }
    }
}
