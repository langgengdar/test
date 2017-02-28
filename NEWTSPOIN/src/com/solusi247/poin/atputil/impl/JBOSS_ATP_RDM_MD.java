package com.solusi247.poin.atputil.impl;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Driver;
import javax.servlet.ServletException;
import oracle.jdbc.pool.OracleDataSource;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;

import com.solusi247.poin.Helper;
import java.util.Calendar;

public class JBOSS_ATP_RDM_MD {
	static private Logger logger = Logger.getLogger(JBOSS_ATP_RDM_MD.class);
	private Helper helper = new Helper();
	private String VREDEEM=null;//            VARCHAR2(100) := NULL;
	private String VSTANDID=null;//           VARCHAR2(100) := NULL;
	private int VJUMSTOCK=0;//          NUMBER := 0;
	private date VLAST_RDM=null;//          DATE   := NULL;
	private HashMap<String, String> hash;
	private String VEVT_TYPE=null;//          VARCHAR2(100) := NULL;
	private String VSTAND_ID=null;//          VARCHAR2(100) := NULL;
	private date VSTART_PERIOD=null;//      DATE := NULL;
	private date VEND_PERIOD=null;//        DATE := NULL;
	private String VPRODUCT=null;//			 VARCHAR2(100) := NULL;
	private int VSTOCK=0;//			 NUMBER := 0;
	private String VSTATUS=null;//			 VARCHAR2(100) := NULL;
	private String VCREATE_DATE=null;//		 VARCHAR2(100) := NULL;
	private String VLAST_UPDATE_STOCK=null;// VARCHAR2(100) := NULL;
	private String VLAST_UPDATE_BY=null;//	 VARCHAR2(100) := NULL;
	private String VLAST_REDEEM=null;//		 VARCHAR2(100) := NULL;
	private date VLAST_RDM_DATE=null;//	 DATE := NULL;
	private String VHEADSTAND=null;//		 ATP_PARAMETER_EVENT.HEADSTAND%TYPE := NULL;
	private String VMAILTO=null;//			 VARCHAR2(100) := NULL;
	private String VMAILCC=null;//			 VARCHAR2(100) := NULL;
	private String VMAILBCC=null;//			 VARCHAR2(100) := NULL;
	private int VDEFAULT_STOCK=0;//	 NUMBER := 0;
	private date VSATOPEN=null;//			 DATE := NULL;
	private date VSUNOPEN=null;//			 DATE := NULL;
	private date VSPEC_OPEN=null;//		 DATE := NULL;
	private date VSPEC_CLOSED=null;//		 DATE := NULL;
	private boolean VHOLIDAY=false;//           BOOLEAN := FALSE;
	private String VTKNID=null;//    		 VARCHAR2(20)  := NULL;
	private String VPARTICIPANT=null;//       VARCHAR2(25)  := NULL;
	private String VZONE=null;//				 VARCHAR2(30)  := NULL;
	private int VMAXRDM=null;//			 NUMBER := NULL;
	private String VEVTID=null;//         ATP_KEYWORDS.EVT_ID_REFF%TYPE              := NULL;
	private int VTOTRDM=null;//			 NUMBER := NULL;
	
	private string VMAX_MODE=null;//    VARCHAR2(50) := NULL;
	private int VPSHIFT=null;//      NUMBER := NULL;
	private int VSPSHIFT=null;//     NUMBER := NULL;
	private int VSTOCKMD=null;//     NUMBER := NULL;
	private string VPROGMODE=null;//    VARCHAR2(50) := NULL;
	private int vSAT_SHIFT=null;//   NUMBER := NULL;
	private int vSUN_SHIFT=null;//   NUMBER := NULL;
	private date vSHIFTOPN=null;//   DATE    := NULL;
	private date vSHIFTCLS=null;//   DATE    := NULL;
	private date vSTARTPRG=null;//   DATE    := NULL;
	private date vENDPRG=null;//     DATE    := NULL;
	private String VSHIFT_TYPE=null;// ATP_PAR_SHIFT.SHIFT_TYPE%TYPE := NULL;
	private int vprogram_id=null;//          ATP_PARAMETER_EVENT.program_id%TYPE   := NULL; --baru
	private string VSTART=null;//     varchar2(20):= NULL;
	private string VEND=null;//       varchar2(20):= NULL;
	private string VRANGE_Z=null;//   VARCHAR2(30) := NULL;
	private string VSEGMENT=null;//   varchar2(50) := NULL;
	private int VSEGMENT2;//  ATP_PARAMETER_EVENT.SEGMENTATION2%TYPE := NULL;
	private string VAREA=null;//      		varchar2(20)  := NULL;
	private string VREGION=null;//      varchar2(20)  := NULL;
	private string vemail_subj=null;//   varchar2(200) := NULL;
	private string vemail_sender=null;// varchar2(100) := NULL;
	private boolean vwhite=false;//        BOOLEAN := FALSE;
	private string vxrowid=null;//       VARCHAR2(50); -- multiwhitelist
	private string vproduct_loop=null;//              varchar2(100) := null;
	private string vprice_loop=null;//              varchar2(100) := null;

	//segmentation variable
	private int vcust_seg=null;//             atp_cust_segmentation.cust_segmentid%TYPE := NULL;
	private String vsegment_name=null;//      atp_cust_segmentation.segment_name%TYPE := NULL;
	private String vreff_type=null;//            atp_cust_segmentation.reff_type%TYPE := NULL;
	private String vreff_name=null;//            atp_cust_segmentation.reff_name%TYPE := NULL;
	private String vreff_column=null;//          atp_cust_segmentation.reff_column%TYPE := NULL;
	private String vdelimiter=null;//            atp_cust_segmentation.delimiter%TYPE := NULL;
	private int vreturn_no=null;//            atp_cust_segmentation.return_no%TYPE := NULL;
	private String vvalid_return=null;//         atp_cust_segmentation.valid_return%TYPE := NULL;
	private int vreturn_multiline=null;//     atp_cust_segmentation.return_multiline%TYPE := NULL;
	private String vprodid=null;//               varchar(10) := null;
	private String vsuccess=null;//              varchar2(100) := null;
	private int vshiftid=null;//              INTEGER := NULL;
	private HashMap<String, Integer> ELM_DAY;

	
	 public void JBOSS_GET_EVT_VARIABLE() throws IOException{
		 	
		 	SimpleDateFormat format1 = new SimpleDateFormat("HHmmss");
		  BEGIN

		    logger.info("begin GET_EVT_VARIABLE ");
		    logger.info("JBOSS_ATP_UTIL.VEVT_ID: " + JBOSS_ATP_UTIL.VEVT_ID);
		    //EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_TERRITORY= ''AMERICA'' NLS_ISO_CURRENCY= ''AMERICA'' NLS_CALENDAR= ''GREGORIAN''  NLS_DATE_LANGUAGE= ''AMERICAN'' NLS_SORT= ''BINARY''';
		    
		    ELM_DAY.put("MON",1);
		    ELM_DAY.put("TUE",1);
		    ELM_DAY.put("WED",1);
		    ELM_DAY.put("THU",1);
		    ELM_DAY.put("FRI",1);
		    ELM_DAY.put("SAT",2);
		    ELM_DAY.put("SAB",2);
		    ELM_DAY.put("SUN",3);	

		    logger.info("cek keyword " + JBOSS_ATP_UTIL.VKEYWORD1);
		    vsql="SELECT EVT_TYPE,CASE WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SATURDAY' AND SAT_OPEN IS NOT NULL AND EVT_TYPE IS NULL THEN SAT_OPEN WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SUNDAY'   AND SUN_OPEN IS NOT NULL AND EVT_TYPE IS NULL THEN SUN_OPEN ELSE START_PERIOD END START_PERIOD,CASE WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SATURDAY' AND SAT_CLOSED IS NOT NULL AND EVT_TYPE IS NULL THEN SAT_CLOSED WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SUNDAY' AND SUN_CLOSED IS NOT NULL AND EVT_TYPE IS NULL THEN SUN_CLOSED ELSE END_PERIOD END END_PERIOD,SAT_OPEN, SUN_OPEN, SPECIAL_OPEN, SPECIAL_CLOSED,PRODUCT, STOCK, STATUS, LAST_REDEEM, LAST_RDM_DATE, HEADSTAND, MAILTO, MAILCC, MAILBCC, DEFAULT_STOCK,PARTICIPANT,nvl(TIME_ZONE,'WIB') TM_ZONE,MAX_REDEeM,EVT_ID,MAX_MODE,CASE WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SATURDAY'AND SAT_SHIFT_MODE IS NOT NULL AND EVT_TYPE IS NULL THEN SAT_SHIFT_MODE WHEN TRIM(TO_CHAR(SYSDATE,'DAY')) = 'SUNDAY'  AND SUN_SHIFT_MODE IS NOT NULL AND EVT_TYPE IS NULL THEN SUN_SHIFT_MODE ELSE REG_SHIFT_MODE END CASE_SYSDATE,SAT_SHIFT_MODE,SUN_SHIFT_MODE,SPEC_SHIFT_MODE, STOCK_MODE, PROGRAM_MODE,START_PERIOD,END_PERIOD,SEGMENTATION, SEGMENTATION2, PROGRAM_ID FROM ATP_PARAMETER_EVENT WHERE EVT_ID = " + JBOSS_ATP_UTIL.VEVT_ID + " AND STATUS  = 1";
		    ResultSet x = statement.executeQuery(vsql);
		    if (rs!=null){
		    	vevt_type=rs.getString(EVT_TYPE);
		    	vstart_period=rs.getString(START_PERIOD);
		    	vend_period=rs.getString(END_PERIOD);
		    	vsatopen=rs.getString(SAT_OPEN);
		    	vsunopen=rs.getString(SUN_OPEN);
		    	vspec_open=rs.getString(SPECIAL_OPEN);
		    	vspec_closed=rs.getString(SPECIAL_CLOSED);
		    	vproduct=rs.getString(PRODUCT);
		    	vstock=rs.getString(STOCK);
		    	vstatus=rs.getString(STATUS);
		    	vlast_redeem=rs.getString(LAST_REDEEM);
		    	vlast_rdm_date=rs.getString(LAST_RDM_DATE);
		    	vheadstand=rs.getString(HEADSTAND);
		    	vmailto=rs.getString(MAILTO);
		    	vmailcc=rs.getString(MAILCC);
		    	vmailbcc=rs.getString(MAILBCC);
		    	vdefault_stock=rs.getString(DEFAULT_STOCK);
		    	vparticipant=rs.getString(PARTICIPANT);
		    	vzone=rs.getString(TM_ZONE);
		    	vmaxrdm=rs.getString(MAX_REDEeM);
		    	vevtid=rs.getString(EVT_ID);
		    	vmax_mode=rs.getString(MAX_MODE);
		    	vpshift=rs.getString(CASE_SYSDATE);
		    	vsat_shift=rs.getString(SAT_SHIFT_MODE);
		    	vsun_shift=rs.getString(SUN_SHIFT_MODE);
		    	vspshift=rs.getString(SPEC_SHIFT_MODE);
		    	vstockmd=rs.getString(STOCK_MODE);
		    	vprogmode=rs.getString(PROGRAM_MODE);
		    	vstartprg=rs.getString(START_PERIOD);
		    	vendprg=rs.getString(END_PERIOD);
		    	vsegment=rs.getString(SEGMENTATION);
		    	vsegment2=rs.getString(SEGMENTATION2);
		    	vprogram_id=rs.getString(PROGRAM_ID);
		    }

		    logger.info("STATUS " + VSTATUS);
		    logger.info("cek keyword " + JBOSS_ATP_UTIL.VKEYWORD1 + " for " + VZONE);

		    VSTAND_ID= JBOSS_ATP_UTIL.VKEYWORD1;
		    logger.info("VPARTICIPANT " + VPARTICIPANT + " " + VLAST_RDM_DATE);

		    JBOSS_ATP_UTIL.VPROD= VPRODUCT;

		    VRANGE_Z= null;
		    if (VZONE =="WITA"){
		    	Calendar calendar = Calendar.getInstance();
		    	calendar.add(Calendar.MINUTE, 60);
		    	VRANGE_Z = format1.format(calendar.getTime());
				//VRANGE_Z = TO_CHAR(SYSDATE + INTERVAL '60' MINUTE, 'HH24MISS');//VRANGE + 1;
		    }else if(ELSIF VZONE=="WIT"){
		    	Calendar calendar = Calendar.getInstance();
		    	calendar.add(Calendar.MINUTE, 120);
		    	VRANGE_Z = format1.format(calendar.getTime());
				//VRANGE_Z=  TO_CHAR(SYSDATE + INTERVAL '120' MINUTE, 'HH24MISS'); //VRANGE + 2;
		    }else{
		    	Calendar calendar = Calendar.getInstance();
		    	VRANGE_Z= format1.format(calendar.getTime());
		    }

			//VRANGE_Z := to_date(VRANGE_Z,'HH24MISS');

		    logger.info("VRANGE_Z => " + VRANGE_Z);


		    BEGIN
		    //cek apakah hari ini adalah hari libur?
		    CHL= null;
					//daftar hari libur
		       CURSOR C_HOLIDAY(XDATE DATE) IS
		       SELECT *
		       FROM   ATP_HOLIDAY
		       WHERE  trunc(HLD_DATE) = XDATE;
		       CHL    C_HOLIDAY%ROWTYPE := NULL;
		    OPEN C_HOLIDAY(TRUNC(SYSDATE));
		    FETCH C_HOLIDAY INTO CHL;
		    CLOSE C_HOLIDAY;
		    EXCEPTION
		    WHEN OTHERS THEN
		    logger.info("TRAP1 => " + sqlerrm);
		    END;

		    logger.info("DY => " + TO_CHAR(SYSDATE,"DY"));

		    BEGIN

		    if (CHL.HLD_ID!=null){
		      VSHIFT_TYPE= 4; //hari ini adalah hari libur
		      logger.info("hari ini adalah hari libur maka VSHIFT_TYPE nya adalah 4");
		    }else{
		      VSHIFT_TYPE = ELM_DAY(TRIM(TO_CHAR(SYSDATE,"DY")));
		      logger.info("hari ini adalah bukan hari libur maka VSHIFT_TYPE nya adalah " + VSHIFT_TYPE);
		    }
		    EXCEPTION
		    WHEN OTHERS THEN
		    logger.info("TRAP2 => " + sqlerrm);
		    END;

		    logger.info("VSHIFT_TYPE => " + VSHIFT_TYPE + " VEVTID:" + VEVTID);

		    BEGIN
		    CSHIFT=null;
			      CURSOR C_SHIFT(Xevt VARCHAR, xshift_type number)
		      is
		      SELECT shift_id, starttime,endtime
		      FROM   atp_par_shift
		      WHERE  evt_id = Xevt
		      AND    shift_mode = xshift_type  //ok
		      AND    vrange_z  BETWEEN To_Char(STARTTIME,'hh24miss') AND To_Char(ENDTIME,'hh24miss');

		      CSHIFT C_SHIFT%ROWTYPE := NULL;

		    OPEN  C_SHIFT(VEVTID, VSHIFT_TYPE);
		    FETCH C_SHIFT INTO vshiftid, vSHIFTOPN,vSHIFTCLS;
		    CLOSE C_SHIFT;

		    EXCEPTION
		    WHEN OTHERS THEN
		    logger.info("TRAP3 => " + sqlerrm);
		    END;


		    logger.info("vSHIFTOPN => " + vSHIFTOPN + " prd:" + vproduct);
		    logger.info("vSHIFTCLS => " + vSHIFTCLS + " prg:" + VPROGRAM_ID);

		    VSTART= null;
		    VEND  = null;

		    VSTART= TO_CHAR(VSTARTPRG,'DD-MM-RRRR');
		    VEND  = TO_CHAR(VENDPRG,'DD-MM-RRRR');


		    if (TRIM(vproduct)== null){
		       JBOSS_ATP_UTIL.VSTATUS = "0";
		       JBOSS_ATP_UTIL.VEVENTMSG = "EVT_PIM_KEYERR";
		       JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg("EVT_PIM_KEYERR");
		       JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		       RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		    }

		  }


			public void CHECK_STOCK() throws IOException{
				logger.info('stock '||vstock);
				if (TRUNC(NVL(VSTOCK,0)) <= 0){
					JBOSS_ATP_UTIL.VSTATUS= "0";
					JBOSS_ATP_UTIL.VEVENTMSG= "EVT_PIM_STOCK_LIMIT";
					JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.JBOSS_getmsg ("EVT_PIM_STOCK_LIMIT");
					JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
					RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
				}
		  }

		  public void CEK_TOT_TRANS_RDM() throws IOException{
		    //* TODO implementation required */
		    null;
		  }

		  public void CEK_TOT_RDM_DAYS() throws IOException{
		    /* TODO implementation required */
		    null;
		  }

		  public void UPD_TRS_RDM() throws IOException{
		    /* TODO implementation required */
		    null;
		  }

		  public void CEK_TOT_RDM_DAYS_NEW() throws IOException{
		      VMSG1 VARCHAR2(100) := NULL;
		      vcurr_sum number= null;
		      String vsql= "SELECT SUM(total) total FROM atp_max_redeem WHERE evt_id = :1 AND msisdn = :2 AND period >= " + to_char(vSTARTPRG,'RRRRMMDD');

		      type ref_cur is ref cursor;
		      cur_sum ref_cur;
		  BEGIN
		      logger.info('CEK_TOT_RDM_DAYS_NEW begin');
		      VMSG1   := NULL;

		      if (vMAX_MODE IN ('2','5')){ // harian
		          vsql= vsql + " and period = '" + to_char(sysdate,'rrrrmmdd') + "'";
		          VMSG1= "EVT_MAX_REDEM_DAY";
		      }else if (vMAX_MODE =="3"){ // monthly
				vsql= vsql + " and substr(period,1,6) = '" + to_char(sysdate,'rrrrmm') + "'";
				VMSG1="EVT_MAX_REDEM_MONTH";
			  }else if(Vmax_modE=="1"){ //shift
				vsql= vsql + " and period = '" + to_char(sysdate,'rrrrmmdd') + "' and shift_id  = '" + vshiftid + "'";
				VMSG1="EVT_MAX_REDEM_SHIFT";
		      /* update by joko
		      ELSIF Vmax_modE ='4' -- program
		      THEN
		       vsql  := 'SELECT sum(total) total FROM atp_max_redeem a, atp_parameter_event b WHERE a.evt_id = b.evt_id and a.evt_id = :1 and a.msisdn = :2 and period >= '||to_char(vSTARTPRG,'RRRRMMDD');
		      ---
		       vmsg1 := 'EVT_MAX_REDEM_PROG';
		       logger.info('prg: '||vsql);
		      END IF;
		      */
		      }else if (Vmax_modE =="4"){ // program
				vsql= "SELECT sum(total) total FROM atp_max_redeem a, atp_parameter_event b WHERE a.evt_id = b.evt_id and b.program_id = :1 and a.msisdn = :2 and period >= " + to_char(vSTARTPRG,'RRRRMMDD');
				vmsg1= 'EVT_MAX_REDEM_PROG';
				logger.info("prg: " + vsql);
		      }

		      logger.info("cek 1 " + vsql);
		      logger.info("JBOSS_ATP_UTIL.VMSISDN: " + JBOSS_ATP_UTIL.VMSISDN);
		      logger.info("vevtid: " + vevtid);
		      logger.info("VSHIFTOPN: " + VSHIFTOPN);
		      logger.info("Vmax_modE: " + Vmax_modE);
		      logger.info("vSTARTPRG: " + vSTARTPRG);

		      /* update by Joko
		      open cur_sum for vsql using vevtid, JBOSS_ATP_UTIL.VMSISDN;
		      fetch cur_sum into vcurr_sum;
		      close cur_sum;
		      */
		      if (Vmax_modE=="4"){
		        open cur_sum for vsql using vprogram_id, JBOSS_ATP_UTIL.VMSISDN;
		        fetch cur_sum into vcurr_sum;
		        close cur_sum;
		      }else{
		        open cur_sum for vsql using vevtid, JBOSS_ATP_UTIL.VMSISDN;
		        fetch cur_sum into vcurr_sum;
		        close cur_sum;
		      }

		      logger.info("cek 2");
		      VTOTRDM= NVL(VCURR_SUM,0) + 1;

		      logger.info("VMSG1=>" + VMSG1);
		      logger.info(" TOTALTRANS=>" + vcurr_sum);

		      if (nvl(vcurr_sum,0) > 0 && NVL(VMAXRDM,0) > 0){
		        if (NVL(VCURR_SUM,0) >= NVL(VMAXRDM,NVL(VCURR_SUM,0)+1)){ //AND VMSG1 IS NOT NULL
		          JBOSS_ATP_UTIL.VSTATUS= "0";
		          JBOSS_ATP_UTIL.VMXRDM= VMAXRDM;
		          JBOSS_ATP_UTIL.VEVENTMSG= VMSG1;
		          JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg("'" + VMSG1 + "'");
		          JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;

		          RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		        }
		      }


		  }

		  public void UPD_TRS_RDM_NEW() throws IOException{
		  BEGIN
		   logger.info('begin UPD_TRS_RDM_NEW');
		   if (VPROGMODE == "1"){  -- harian
				logger.info("update harian");
			   UPDATE ATP_MAX_REDEEM
			   SET   TOTAL        = TOTAL+1
			   	    ,LAST_UPDATED = SYSDATE
			   WHERE MSISDN   = JBOSS_ATP_UTIL.VMSISDN
			   AND 	 EVT_ID   = VEVTID
		     AND   SHIFT_ID = 0
		     AND   PERIOD   = TO_CHAR(SYSDATE,'RRRRMMDD');

			   IF SQL%NOTFOUND
			   THEN
		        logger.info("insert harian");
			      BEGIN
		  		    INSERT INTO  ATP_MAX_REDEEM  (EVT_ID, SHIFT_ID, MSISDN, TOTAL, PERIOD, CREATED_DATE, LAST_UPDATED)
		    			VALUES (VEVTID, 0, JBOSS_ATP_UTIL.VMSISDN, 1, TO_CHAR(SYSDATE,'RRRRMMDD'), SYSDATE,SYSDATE);
		  		  END;
			   END IF;
		   }else if(VPROGMODE == "2"){ // shift
		     BEGIN
		     logger.info("update shift VEVTID:" + VEVTID + " VSHIFTID:" + VSHIFTID + " VTOTRDM:" + VTOTRDM);
			   UPDATE ATP_MAX_REDEEM
			   SET TOTAL        = TOTAL+1
			   	  ,LAST_UPDATED = SYSDATE
			   WHERE MSISDN     = JBOSS_ATP_UTIL.VMSISDN
			   AND 	 EVT_ID     = VEVTID
		     AND   PERIOD     = TO_CHAR(SYSDATE,'RRRRMMDD')
		     AND   SHIFT_ID   = VSHIFTID;//TO_CHAR(VSHIFTOPN,'HH24MI');
			   IF SQL%NOTFOUND
			   THEN
		        logger.info("insert shift VEVTID:" + VEVTID + " VSHIFTID:" + VSHIFTID + " VTOTRDM:" + VTOTRDM);
			      BEGIN
		  		    INSERT INTO  ATP_MAX_REDEEM  (EVT_ID, SHIFT_ID, MSISDN, TOTAL, PERIOD, CREATED_DATE, LAST_UPDATED)
						  		VALUES (VEVTID, VSHIFTID, JBOSS_ATP_UTIL.VMSISDN, 1, TO_CHAR(SYSDATE,'RRRRMMDD'), SYSDATE, SYSDATE);
		  		  END;
			   END IF;
		     END;
		   }

		   logger.info("end UPD_TRS_RDM_NEW");
		  }

		  public void RESTORE_STOCK() throws IOException{

		    int VNUM =null;//        := NULL;
		    String VSQL=null;//  VARCHAR2(2000) := NULL;

		  BEGIN
		    logger.info("VSTOCKMD=>" + VSTOCKMD);


		    if (VSTOCKMD== 2){  // update stock per shift
		        logger.info("vSHIFTOPN=>" + vSHIFTOPN + " , vSHIFTCLS=>" + vSHIFTCLS);
		        if (Nvl(To_Char(VLAST_RDM_DATE,'hh24miss'),To_Char(vSHIFTOPN,'hh24miss')) < To_Char(vSHIFTOPN,'hh24miss')){
		            VNUM= 1;
		        }else if(Trunc(VLAST_RDM_DATE) < TRUNC(SYSDATE)){
		           VNUM= 1;
		        }
		    }else if(VSTOCKMD ==3){//persediaan harian
		        VNUM= (TRUNC(SYSDATE)-TRUNC(NVL(VLAST_RDM_DATE,TO_DATE('01012008','DDMMRRRR'))));
		        logger.info("vnum1 " + VNUM);
		    }else if  (VSTOCKMD == 5){ // update stock per hari dan daily carried over --persediaan harian berlanjut
		        VNUM = (TRUNC(SYSDATE)-TRUNC(NVL(VLAST_RDM_DATE,SYSDATE)));
		        logger.info("vnum carry over " + VNUM);
		    }else if (VSTOCKMD == 4){ //Persediaan Berlanjut(carried over)
		        VNUM= 0;//VSTOCK;
		    }else{
		          //Tanpa Persedian(stock),
		          VSTOCK= 1;
		          VNUM= 0;
		    }

		    logger.info("vnum " + VNUM);

		    logger.info("VDEFAULT_STOCK " + VDEFAULT_STOCK);

		     if (VNUM >= 1){
		        if (VDEFAULT_STOCK==null){
		           VSQL := 'UPDATE ATP_PARAMETER_EVENT SET STOCK = DEFAULT_STOCK WHERE EVT_ID = :1';
		        }else{
		           if (VSTOCKMD == 5){ //daily carried over
		              VSQL= "UPDATE ATP_PARAMETER_EVENT SET STOCK = STOCK+DEFAULT_STOCK, PARTICIPANT = 0 WHERE EVT_ID = :1";
		              VPARTICIPANT= 0;
		           }else{
		              VSQL= "UPDATE ATP_PARAMETER_EVENT SET STOCK = DEFAULT_STOCK, PARTICIPANT = 0 WHERE EVT_ID = :1";
		              VPARTICIPANT= 0;
		           }
		        }

		        logger.info(vsql);

		        EXECUTE IMMEDIATE VSQL USING JBOSS_ATP_UTIL.VEVT_ID;
		        logger.info("UPDATE STOCK TO DEFAULT WITH EVT_ID " + JBOSS_ATP_UTIL.VEVT_ID);
		        COMMIT;
		        VSTOCK = VDEFAULT_STOCK;
		     }



		     JBOSS_ATP_UTIL.VNU_TXT= VPARTICIPANT+1;
		     logger.info("PARTISIPAN " + JBOSS_ATP_UTIL.VNU_TXT);

		  }

		  public void HOLIDAY_REDEEMPTION() throws IOException{
		  BEGIN
		  if (VSPEC_OPEN !=null){
		     VDATE = TRUNC(SYSDATE);
			      CURSOR C_HOLIDAY(XDATE DATE) IS
		      SELECT *
		      FROM   ATP_HOLIDAY
		      WHERE  HLD_DATE = XDATE;
		      CHL    C_HOLIDAY%ROWTYPE = null;
		     OPEN  C_HOLIDAY(VDATE);
		     FETCH C_HOLIDAY INTO CHL;
		     CLOSE C_HOLIDAY;

		     if (CHL.HLD_DATE!=null){// today is a holiday then raised
		      if (VPROGMODE =="2"){
		            CSHIFT= null;
					
		      CURSOR C_SHIFT(XEVTID NUMBER)
		      IS
		      SELECT *
		      FROM ATP_PAR_SHIFT
		      WHERE EVT_ID = XEVTID
		      AND SHIFT_TYPE = '4'
		      AND VRANGE_Z  BETWEEN To_Char(STARTTIME,'hh24miss')  // VRANGE_Z => To_Char(SYSDATE,'hh24miss')
		      AND To_Char(ENDTIME,'hh24miss');

		      CSHIFT C_SHIFT%ROWTYPE := null;
		            OPEN  C_SHIFT(VEVTID);
		            FETCH C_SHIFT INTO CSHIFT;
		            CLOSE C_SHIFT;

		            if (VSPSHIFT==null){ // CEK  HOLIDAY ADA YG BUKA ATAU TIDAK
		              JBOSS_ATP_UTIL.VSTATUS= "0";
		              JBOSS_ATP_UTIL.VEVENTSET_TXT= JBOSS_ATP_UTIL.VKEYWORD;
		              JBOSS_ATP_UTIL.vprod= VPRODUCT;
		              JBOSS_ATP_UTIL.VEVENTMSG= "EVT_HOLIDAY_NA";
		              JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_HOLIDAY_NA");
		              JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		              RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		            }else if (CSHIFT.SHIFT_TYPE==null){   //CSHIFT.SHIFT_MODE IS NULL   ---- CEK SHIFT HOLIDAY ADA YG BUKA ATAU TIDAK
		              JBOSS_ATP_UTIL.VSTATUS = "0";
		              JBOSS_ATP_UTIL.VTGL_TXT= CHECK_NEXTSHIFT("4"); //TO_CHAR(VSTARTPRG,'DD/MM/RR');
		              JBOSS_ATP_UTIL.VEVENTMSG = "EVT_SHIFT_CLS_DATE";
		              JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg ("EVT_SHIFT_CLS_DATE");
		              JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		              RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		            }else{
		              VHOLIDAY= TRUE;
		              VSTART_PERIOD= CSHIFT.STARTTIME;
		              VEND_PERIOD= CSHIFT.ENDTIME;
		            }

		      }else{

		        CXC = null;
				      CURSOR C_EXCLUDE(XKEYWORD VARCHAR2, XDATE DATE) IS
		      SELECT *
		      FROM   ATP_EXCLUDE_HOLIDAY
		      WHERE  KEYWORD  = XKEYWORD
		      AND	  EXC_DATE = XDATE;
		      CXC	  C_EXCLUDE%ROWTYPE := NULL;
		      VDATE  DATE := NULL;
		        OPEN  C_EXCLUDE(JBOSS_ATP_UTIL.VKEYWORD1,VDATE);
		        FETCH C_EXCLUDE INTO CXC;
		        CLOSE C_EXCLUDE;

		            if (CXC.KEYWORD == null && VSPEC_OPEN ==null){// no exclude
		              JBOSS_ATP_UTIL.VSTATUS= "0";
		              JBOSS_ATP_UTIL.VEVENTSET_TXT = JBOSS_ATP_UTIL.VKEYWORD;
		              JBOSS_ATP_UTIL.vprod = VPRODUCT;
		              JBOSS_ATP_UTIL.VEVENTMSG = "EVT_HOLIDAY_NA";
		              JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg ("EVT_HOLIDAY_NA");
		              JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		              RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		            }else{
		              //replace time open/closed for holiday ---
		              logger.info("its time for holiday");
		              VHOLIDAY= TRUE;
		              VSTART_PERIOD= VSPEC_OPEN;
		              VEND_PERIOD= VSPEC_CLOSED;
		            }
			  }
		     }
		  }
		  }

			public void WEEKLY_REDEEMPTION() throws IOException{

		      CURSOR c_shift(XSHIFT NUMBER)
		      is
		      SELECT *
		      FROM   atp_par_shift
		      WHERE  evt_id = vevtid
		      AND    shift_mode = xshift
		      AND    vrange_z BETWEEN To_Char(STARTTIME,'hh24miss') AND To_Char(ENDTIME,'hh24miss');

		      CSHIFT C_SHIFT%ROWTYPE := NULL;

		      CURSOR c_shifday(xsfhift VARCHAR2) IS
		      SELECT *
		      FROM   atp_par_shift
		      WHERE  evt_id = vevtid
		      AND    shift_mode = xsfhift;
		      csfhdy c_shifday%ROWTYPE := NULL;

		      vxtime date = TO_DATE("01042013" + TO_CHAR(SYSDATE,'HH24MISS'),'DDMMRRRRHH24MISS');
		      String vxmode;

		  BEGIN
			logger.info("VPROGMODE = " + VPROGMODE);
			logger.info("VPSHIFT = " + VPSHIFT);
			if (VPROGMODE =="2"){ // shift
		      vxmode= NULL;
		      if (TRIM(TO_CHAR(SYSDATE,'DAY')) = "SATURDAY"){
		         vxmode = "2";
		      }else if(TRIM(TO_CHAR(SYSDATE,'DAY')) = "SUNDAY"){
		         vxmode= "3";
		      }

		      if (vxmode!=null){
				logger.info("vxmode = " + vxmode);
				csfhdy =null;
				OPEN c_shifday(vxmode);
				FETCH c_shifday INTO csfhdy;
				CLOSE c_shifday;

		      if (csfhdy.SHIFT_MODE!=null){
		           Cshift= null;
		           OPEN  c_shift(vxmode);
		           FETCH c_shift INTO cshift;
		           CLOSE c_shift;

		           vshiftid = cshift.shift_id;

		           if (cshift.starttime==null){ //IS NULL  -- kalo belum buka
		              JBOSS_ATP_UTIL.VSTATUS= "0";
		              JBOSS_ATP_UTIL.VTGL_TXT= atp_rdm_md.check_nextshift(vxmode); ---TO_CHAR(VSTARTPRG,'DD/MM/RR');
		              JBOSS_ATP_UTIL.VEVENTMSG= "EVT_SHIFT_CLS_DATE";
		              JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_SHIFT_CLS_DATE");
		              JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		              RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		           }
			  }else{
		            JBOSS_ATP_UTIL.VSTATUS= "0";
		            JBOSS_ATP_UTIL.VEVENTSET_TXT= JBOSS_ATP_UTIL.VKEYWORD;
		            JBOSS_ATP_UTIL.VPROD= VPRODUCT;
		            JBOSS_ATP_UTIL.VEVENTMSG= "EVT_HOLIDAY_NA";
		            JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_HOLIDAY_NA");
		            JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		            RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
			  }
		      }

			}else{

		      if (TRIM(TO_CHAR(SYSDATE,'DAY')) == "SATURDAY"){
				logger.info("saturday check :" + VSATOPEN);
		        if (VSATOPEN==null && VHOLIDAY ==false){//redeem hari sabtu tidak boleh
		            //raised
		            JBOSS_ATP_UTIL.VSTATUS= "0";
		            JBOSS_ATP_UTIL.VEVENTSET_TXT= JBOSS_ATP_UTIL.VKEYWORD;
		            JBOSS_ATP_UTIL.VPROD= VPRODUCT;
		            JBOSS_ATP_UTIL.VEVENTMSG="EVT_HOLIDAY_NA";
		            JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg("EVT_HOLIDAY_NA");
		            JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		            RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
				}
		      }else if (TRIM(TO_CHAR(SYSDATE,'DAY')) = "SUNDAY"){
		        logger.info("sunday check :" + VSUNOPEN);
		         logger.info("VEVTID sunday " + VEVTID);
		        if (VSUNOPEN==null && VHOLIDAY ==false){ //redeem hari minggu tidak boleh. False mean no exclude
		            //raised
		            JBOSS_ATP_UTIL.VSTATUS= "0";
		            JBOSS_ATP_UTIL.VEVENTSET_TXT= JBOSS_ATP_UTIL.VKEYWORD;
		            JBOSS_ATP_UTIL.VPROD= VPRODUCT;
		            JBOSS_ATP_UTIL.VEVENTMSG= "EVT_HOLIDAY_NA";
		            JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_HOLIDAY_NA");
		            JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		            RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
				}
		      }
			}
			}

		  public void CHECK_EVT_PERIOD() throws IOException{

		  BEGIN
		    logger.info("VPROGMODE=>" + VPROGMODE);

		    if (VPROGMODE =="2"){
		        logger.info("VEVTID=>" + VEVTID);
		        logger.info("VSHIFT_TYPE=>" + VSHIFT_TYPE);

		         CSHIFT=null;
				   CURSOR C_SHIFT(Xevtid varchar2, XSHIFT_TYPE varchar2)
		  IS
		  SELECT *
		  FROM  ATP_PAR_SHIFT
		  WHERE EVT_ID = Xevtid
		  AND   SHIFT_MODE = XSHIFT_TYPE
		  AND   VRANGE_Z BETWEEN  To_Char(STARTTIME,'hh24miss') AND To_Char(ENDTIME,'hh24miss');


		  CSHIFT C_SHIFT%ROWTYPE := NULL;

		         OPEN  C_SHIFT(VEVTID, VSHIFT_TYPE);
		         FETCH C_SHIFT INTO CSHIFT;
		         CLOSE C_SHIFT;

		         logger.info("CSHIFT.SHIFT_TYPE=> " + CSHIFT.SHIFT_TYPE + " FOR " + VRANGE_Z);

		         if (TRUNC(SYSDATE) NOT BETWEEN TRUNC(VSTARTPRG) AND TRUNC(VENDPRG)){
		            logger.info("SYSDATE not between =>" + VSTARTPRG + " and " + VENDPRG);
		            JBOSS_ATP_UTIL.VSTATUS= "0";
		            JBOSS_ATP_UTIL.VTGL_TXT= TO_CHAR(VSTARTPRG,'DD/MM/RR');
		            JBOSS_ATP_UTIL.VBLN_TXT= TO_CHAR(VendPRG,'DD/MM/RR');
		            JBOSS_ATP_UTIL.VEVENTMSG= "EVT_STAND_CLS_HOU";
		            JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_STAND_CLS_HOU");
		            JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		            RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		         }else{
		              logger.info("SYSDATE between =>" + VSTARTPRG + " and " + VENDPRG);
		             if (CSHIFT.SHIFT_TYPE ==null && TRIM(TO_CHAR(SYSDATE,'DAY')) NOT IN ('SATURDAY','SUNDAY')){
		                logger.info("CSHIFT.SHIFT_TYPE=null &&  =>" + TO_CHAR(SYSDATE,'DAY') + " not in saturday,sunday ");
		                 csfhdy=null;
						   CURSOR c_shifday IS
		  SELECT *
		  FROM ATP_PAR_SHIFT
		  WHERE EVT_ID = VEVTID
		  AND   SHIFT_MODE = 1;
		  csfhdy c_shifday%ROWTYPE := NULL;
		                 OPEN c_shifday;
		                 FETCH c_shifday INTO csfhdy;
		                 CLOSE c_shifday;

		                logger.info("csfhdy.SHIFT_TYPE:" + csfhdy.SHIFT_TYPE);
		                if (csfhdy.SHIFT_TYPE!=null){
		                    logger.info(TRUNC(SYSDATE+1) + " NOT BETWEEN " + TRUNC(VSTARTPRG) + " AND " + TRUNC(VENDPRG));
		                   if (TRUNC(SYSDATE+1) NOT BETWEEN TRUNC(VSTARTPRG) AND TRUNC(VENDPRG)){
		                    logger.info("TRUNC(SYSDATE+1) NOT BETWEEN TRUNC(VSTARTPRG) AND TRUNC(VENDPRG)");
		                   //CEK PRIODE EVENT
		                      JBOSS_ATP_UTIL.VSTATUS= "0";
		                      JBOSS_ATP_UTIL.VTGL_TXT= TO_CHAR(VSTARTPRG,'DD/MM/RR');
		                      JBOSS_ATP_UTIL.VBLN_TXT= TO_CHAR(VendPRG,'DD/MM/RR');
		                      JBOSS_ATP_UTIL.VEVENTMSG = "EVT_STAND_CLS_HOU";
		                      JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_STAND_CLS_HOU");
		                      JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		                      RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		                   }else{

		                    JBOSS_ATP_UTIL.VSTATUS= "0";
		                    JBOSS_ATP_UTIL.VTGL_TXT= CHECK_NEXTSHIFT(VSHIFT_TYPE); //TO_CHAR(VSTARTPRG,'DD/MM/RR');
		                    JBOSS_ATP_UTIL.VEVENTMSG= "EVT_SHIFT_CLS_DATE";
		                    JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg ("EVT_SHIFT_CLS_DATE");
		                    JBOSS_ATP_UTIL.VMSG_OUT  = JBOSS_ATP_UTIL.VERRMSG;
		                    RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		                   }

		                }else{
		                  JBOSS_ATP_UTIL.VSTATUS= "0";
		                  JBOSS_ATP_UTIL.VEVENTSET_TXT= JBOSS_ATP_UTIL.VKEYWORD;
		                  JBOSS_ATP_UTIL.VPROD= VPRODUCT;
		                  JBOSS_ATP_UTIL.VEVENTMSG= "EVT_DAYS_NA";
		                  JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg ("EVT_DAYS_NA");
		                  JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		                  RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		                }

					 }
		         }
		    }else{
		      //logger.info('PROG 1 : compare '||TO_CHAR(SYSDATE,'DDMMRRRR')||VRANGE_Z||' with '||to_char(VSTART_PERIOD,'dd-mm-rrrr hh24:mi:ss')||' s/d '||to_char(VEND_PERIOD,'dd-mm-rrrr hh24:mi:ss'));
		      if (TO_DATE(TO_CHAR(SYSDATE,'DDMMRRRR')+VRANGE_Z,'DDMMRRRRHH24MISS') NOT BETWEEN VSTART_PERIOD AND VEND_PERIOD){
		        JBOSS_ATP_UTIL.VSTATUS = "0";
		        JBOSS_ATP_UTIL.VTGL_TXT = TO_CHAR(VSTART_PERIOD,'DD/MM/RR HH24:MI');
		        JBOSS_ATP_UTIL.VBLN_TXT = TO_CHAR(VEND_PERIOD,'DD/MM/RR HH24:MI') + " " + VZONE;
		        JBOSS_ATP_UTIL.VEVENTMSG = "EVT_STAND_CLS_HOU";
		        JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ("EVT_STAND_CLS_HOU");
		        JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		        RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		      }
		    }

		  }

		  public void CHECK_NEXTSHIFT(String P_shift) throws IOException{



		    String VRET=null;// VARCHAR2(50) := NULL;

		  BEGIN

			logger.info("GET NEXT SHIFT");
		     VRET=null;
				    CURSOR c_par
					IS
					SELECT *
					FROM ATP_PAR_SHIFT
					WHERE EVT_ID = VEVTID
					AND SHIFT_MODE LIKE Nvl(P_shift,'%')
					ORDER BY STARTTIME ASC;
		         FOR i IN c_par
		         LOOP
		             if (VRANGE_Z  < to_char(I.starttime,'hh24miss')) {// VRANGE_Z => to_char(sysdate,'hh24miss')
		                 logger.info("VRANGE_Z " + VRANGE_Z + "  i.start => " + to_char(I.starttime,'hh24miss'));
		                 VRET = to_char(I.starttime,'hh24:mi:ss') + " " + VZONE;
		                 EXIT;
		             }
		         END LOOP;

		      if (VRET==null){
		          cshift =null;
				      CURSOR c_shift (xmode VARCHAR2)
		    IS
		    SELECT Min(STARTTIME) TGL, TO_CHAR(NEXT_DAY(SYSDATE,CASE WHEN xmode='2' THEN 'SATURDAY' WHEN xmode = '3' THEN 'SUNDAY' END),'DD/MM/RRRR') nextdate
		    FROM  ATP_PAR_SHIFT
		    WHERE EVT_ID = VEVTID
		    AND   SHIFT_MODE = xmode;
		          OPEN  c_shift(ELM_DAY(TO_CHAR(SYSDATE+1,'DY')));
		          FETCH c_shift INTO cshift;
		          CLOSE c_shift;

		          VRET = "Jam " + TO_CHAR(cshift.tgl,'hh24:mi:ss');
		          if (Trim(Replace(vret,'Jam'))==null){
		             FOR xx IN 2..3
		             LOOP
		                cshift= null;
						    CURSOR c_nextshift (xmode VARCHAR2)
		    IS
		    SELECT Min(STARTTIME) TGL, TO_CHAR(NEXT_DAY(SYSDATE,CASE WHEN xmode='2' THEN 'SATURDAY' WHEN xmode = '3' THEN 'SUNDAY' END),'DD/MM/RRRR') nextdate
		    FROM  ATP_PAR_SHIFT
		    WHERE EVT_ID = VEVTID;
		                OPEN  c_nextshift(xx);
		                FETCH c_nextshift INTO cshift;
		                CLOSE c_nextshift;

		                vret= cshift.nextdate + " " + TO_CHAR(cshift.tgl,'hh24:mi:ss');
		                if (vret!=null){
		                   EXIT;
						}
		             END LOOP;

		          }

		      }

		      return VRET + " " + VZONE;
		  }


			public void CHECK_TIME_PERIOD() throws IOException{
			String VRANGE=null;
			String VST_RANGE=null;
			String VEND_RANGE=null;

			String vSTRDT=null;
			String vENDT=null;
			BEGIN
			VRANGE =null;

			 if (VZONE == "WITA"){
				VRANGE = TO_CHAR(SYSDATE + INTERVAL '60' MINUTE, 'HH24');//VRANGE + 1;
			 }else if (VZONE =="WIT"){
				VRANGE =  TO_CHAR(SYSDATE + INTERVAL '120' MINUTE, 'HH24'); //VRANGE + 2;
			 }else{
			 	VRANGE= TO_CHAR(SYSDATE,'HH24');
			 }

			VRANGE = VRANGE + TO_CHAR(SYSDATE,'MI'); //JAMMENIT

			logger.info("VSTART_PERIOD:" + VSTART_PERIOD);
			logger.info("VSTART_PERIOD:" + TO_CHAR(VSTART_PERIOD,'HH24MI'));
			logger.info("VEND_PERIOD:" + VEND_PERIOD);

			if (VPROGMODE =="2"){ //SHIFT
		      vSTRDT=  TO_CHAR(vSHIFTOPN,'HH24MI');
		      vENDT =  TO_CHAR(vSHIFTCLS,'HH24MI');
			}else{ //HARIAN
		      vSTRDT= TO_CHAR(VSTART_PERIOD,'HH24MI');
		      vENDT= TO_CHAR(VEND_PERIOD,'HH24MI');
			}

				logger.info("vrange:" + vrange);
				logger.info("vSTRDT:" + vSTRDT);
				logger.info("vENDT:" + vENDT);

		   if (SYSDATE NOT BETWEEN VSTART_PERIOD AND VEND_PERIOD){
		      JBOSS_ATP_UTIL.VSTATUS= "0";
			    JBOSS_ATP_UTIL.VZONE=  VZONE;
		      JBOSS_ATP_UTIL.VJAM=  TO_CHAR(To_Date(vSTRDT,'HH24:MI'),'HH24:MI');
		      JBOSS_ATP_UTIL.VNMPRODUK=  TO_CHAR(TO_date(vENDT,'HH24:MI'),'HH24:MI');
		      JBOSS_ATP_UTIL.VEVENTMSG= 'EVT_STAND_CLS_HOUR';
		      JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg ('EVT_STAND_CLS_HOUR');
		      JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		      RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		   }
		}

		  public void UPDATE_STOCK() throws IOException{
		  String VSQL=null;
		  BEGIN


		    if (VSTOCKMD == 1){
		       VSQL = " UPDATE ATP_PARAMETER_EVENT SET PARTICIPANT   = PARTICIPANT + 1,LAST_REDEEM   = '" + JBOSS_ATP_UTIL.VMSISDN + "', LAST_RDM_DATE = SYSDATE WHERE  EVT_ID = :1";
		    }else{
		       VSQL = "UPDATE ATP_PARAMETER_EVENT SET STOCK = STOCK - 1,PARTICIPANT   = PARTICIPANT + 1,LAST_REDEEM   = '" + JBOSS_ATP_UTIL.VMSISDN + "', LAST_RDM_DATE = SYSDATE WHERE  EVT_ID = :1";

		    }

		    EXECUTE IMMEDIATE VSQL USING JBOSS_ATP_UTIL.VEVT_ID;



		   logger.info("UPDATE OK " +  JBOSS_ATP_UTIL.VKEYWORD1 + " " + VREDEEM);
		   JBOSS_ATP_UTIL.VVALUE_TXT= JBOSS_ATP_UTIL.VPOINT_USG;
		   JBOSS_ATP_UTIL.VPROD= VREDEEM;
		   JBOSS_ATP_UTIL.VPROG_NAME= JBOSS_ATP_UTIL.VKEYWORD;
		   JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.VPROG_NAME);
		  }

		  public void UPDATE_PARTICIPANTS() throws IOException{
		  
		    /* TODO implementation required */
		    null;
		  }

		  public void NOTIF_HEADSTAND() throws IOException{
		      String VTXT=null;
		      int VSBAR=0;
		      String vstr=null;
		      int vctr=0;
		      String vmsisdn_pic=null;
		      String vheadstand_temp=null;// atp_parameter_event.headstand%type := null;
		  BEGIN
		    if (VHEADSTAND!=null){
		     vheadstand_temp = case when substr(vheadstand,length(vheadstand),1) = ';' then substr(vheadstand,1,length(vheadstand)-1) else vheadstand end;
		     vheadstand_temp = trim(vheadstand_temp);

		    FOR a IN 1..Length(vheadstand_temp||';')
		    LOOP
		        vstr := Substr(vheadstand_temp||';',a,1);
		        if (vstr == ";"){
		           vctr = vctr + 1;
		        }
		    END LOOP;
		    logger.info("total char:" + vctr);

		    logger.info("NOTIF_HEADSTAND " + JBOSS_ATP_UTIL.VKEYWORD1 + " " + VREDEEM);
		    JBOSS_ATP_UTIL.VTGL_TXT = null;
		    JBOSS_ATP_UTIL.VDEST_TXT = null;
		    JBOSS_ATP_UTIL.VVALUE_TXT = null;
		    ///
		    JBOSS_ATP_UTIL.VDEST_TXT = JBOSS_ATP_UTIL.VMSISDN;//VHEADSTAND;
		    JBOSS_ATP_UTIL.VTP_TXT  = "0" + JBOSS_ATP_UTIL.VMSISDN;
		    JBOSS_ATP_UTIL.VVALUE_TXT = JBOSS_ATP_UTIL.VPOINT_USG;
		    JBOSS_ATP_UTIL.VPROD = VPRODUCT;
		    JBOSS_ATP_UTIL.VTGL_TXT = TO_CHAR(SYSDATE,'DD/MM/RR HH24:MI');

		    logger.info("VPROD " + JBOSS_ATP_UTIL.VPROD + " tgl:" + JBOSS_ATP_UTIL.VTGL_TXT);
		    VTXT = "PIM_STOCK";
		    logger.info("send sms to PIC: " + vtxt + ":" + vheadstand_temp);

		    FOR b IN 1..vctr
		    LOOP
		       vmsisdn_pic= null;
		       vmsisdn_pic = get_delimitedtext(vheadstand_temp,';', b);
		       JBOSS_ATP_UTIL.sendsms_non(vtxt, vmsisdn_pic,JBOSS_ATP_UTIL.vprogram_id);
		       atp_rdm_md.logging_sms_mt(p_msisdn=>vmsisdn_pic, p_msg=>JBOSS_ATP_UTIL.getmsg(vtxt), p_event=>vtxt);
		    END LOOP;
			}

		   JBOSS_ATP_UTIL.VTGL_TXT= NULL;
		   JBOSS_ATP_UTIL.VTGL_TXT= TO_CHAR(SYSDATE,'DD/MM/RR HH24:MI');

		  }

		  public void VALIDASI_POIN()throws IOException{
		    JBOSS_ATP_UTIL.VVALUE_TXT = TRIM (JBOSS_ATP_UTIL.VPOINT_VALUE);
		    JBOSS_ATP_UTIL.VPOIN_TXT = TRIM (JBOSS_ATP_UTIL.VPOINT_USG);
		    logger.info ("JBOSS_ATP_UTIL.VEVT_GROUP_ID: " + JBOSS_ATP_UTIL.VEVT_GROUP_ID);
		    JBOSS_ATP_UTIL.VPOINT = JBOSS_ATP_UTIL.GET_POINT_INFO (JBOSS_ATP_UTIL.VSBR_ID, JBOSS_ATP_UTIL.VMSISDN, JBOSS_ATP_UTIL.VEVT_GROUP_ID);


		    logger.info ("JUMLAH POIN=" + JBOSS_ATP_UTIL.vpoint + " FOR SBR=" + JBOSS_ATP_UTIL.vsbr_id);
		    if (JBOSS_ATP_UTIL.VPOINT < JBOSS_ATP_UTIL.VPOINT_USG){
		      JBOSS_ATP_UTIL.vstatus = "0";
		      JBOSS_ATP_UTIL.veventset_txt = JBOSS_ATP_UTIL.vkeyword;
		      JBOSS_ATP_UTIL.vvalue_txt = JBOSS_ATP_UTIL.VPOINT_USG;
		      JBOSS_ATP_UTIL.veventmsg = "EVT_PIM_POIN_LIMIT";
		      JBOSS_ATP_UTIL.verrmsg = JBOSS_ATP_UTIL.getmsg ("EVT_PIM_POIN_LIMIT");
		      JBOSS_ATP_UTIL.vmsg_out = JBOSS_ATP_UTIL.verrmsg;
		      RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.verrmsg);
		    }
		  }

		  public void MEMBER_PRIOR_CHECK()throws IOException{
		    /* TODO implementation required */
		    null;
		  }

		  public void REQ_CHECK_MEMBER(String P_MSISDN)throws IOException{
		    /* TODO implementation required */
		    return null;
		  }

		  public void WRITE_TRANSACTION()throws IOException{
		    /* TODO implementation required */
			null;
		  }

		  public void REQ_INSERT_TRS(String P_TKNID,String P_MSISDN)throws IOException{
		    /* TODO implementation required */
		    return null;
		  }

		  public void CHECK_STOCK_PIC()throws IOException{
		        logger.info("stock " + vstock);
		      if (TRUNC(NVL(VSTOCK,0)) <= 0){
		        JBOSS_ATP_UTIL.VSTATUS = "0";
		        JBOSS_ATP_UTIL.VEVENTMSG = "EVT_PIM_STOCK_LIMIT";
		        JBOSS_ATP_UTIL.VERRMSG = JBOSS_ATP_UTIL.getmsg ("EVT_PIM_STOCK_LIMIT");
		        JBOSS_ATP_UTIL.VMSG_OUT = JBOSS_ATP_UTIL.VERRMSG;
		        RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		      }
		  }

		  public void CUST_SEGMENTATION()throws IOException{

		      String vsql= null;
		      String va= null;
		      String vretval= null;
		      String vresp= null;
		      boolean vskip=false;

		      TYPE        reff_cursor IS REF CURSOR;
		      c_q         reff_cursor;

		  	//TYPE arr_segment_id		     IS TABLE OF atp_par_segmentation.cust_segmentid%TYPE     INDEX BY BINARY_INTEGER;
		  	//TYPE arr_segment_name		   IS TABLE OF atp_par_segmentation.segment_name%TYPE		    INDEX BY BINARY_INTEGER;
		  	//TYPE arr_reff_name         IS TABLE OF atp_cust_segmentation.reff_name%TYPE    	    INDEX BY BINARY_INTEGER;
		  	//TYPE arr_reff_column       IS TABLE OF atp_cust_segmentation.reff_column%TYPE       INDEX BY BINARY_INTEGER;
		  	//TYPE arr_delimiter         IS TABLE OF atp_cust_segmentation.delimiter%TYPE      	  INDEX BY BINARY_INTEGER;
		  	//TYPE arr_reff_type         IS TABLE OF atp_cust_segmentation.reff_type%TYPE       	INDEX BY BINARY_INTEGER;
		  	//TYPE arr_prefix_digit      IS TABLE OF atp_cust_segmentation.prefix_digit%TYPE      INDEX BY BINARY_INTEGER;
		  	//TYPE arr_valid_return      IS TABLE OF atp_cust_segmentation.valid_return%TYPE    	INDEX BY BINARY_INTEGER;
		  	//TYPE arr_return_no         IS TABLE OF atp_cust_segmentation.return_no%TYPE    	    INDEX BY BINARY_INTEGER;
		  	//TYPE arr_return_multiline  IS TABLE OF atp_cust_segmentation.return_multiline%TYPE  INDEX BY BINARY_INTEGER;
		  	//TYPE arr_segment_value     IS TABLE OF atp_par_segmentation.segment_value%TYPE      INDEX BY BINARY_INTEGER;

			
				arraylist elm_segment_id=new arraylist();
				arraylist elm_segment_name=new arraylist();
				arraylist elm_reff_name=new arraylist();
				arraylist elm_reff_column=new arraylist(); 
				arraylist elm_delimiter=new arraylist(); 
				arraylist elm_reff_type=new arraylist(); 
				arraylist elm_prefix_digit=new arraylist(); 
				arraylist elm_valid_return=new arraylist(); 
				arraylist elm_return_no=new arraylist(); 
				arraylist elm_return_multiline=new arraylist(); 
				arraylist elm_segment_value=new arraylist();
				
				//elm_segment_id  		    arr_segment_id;
				//elm_segment_name		    arr_segment_name;
				//elm_reff_name           arr_reff_name;
				//elm_reff_column 		    arr_reff_column;
				//elm_delimiter 			    arr_delimiter;
				//elm_reff_type 			    arr_reff_type;
				//elm_prefix_digit 		    arr_prefix_digit;
				//elm_valid_return 		    arr_valid_return;
				//elm_return_no 			    arr_return_no;
				//elm_return_multiline 	  arr_return_multiline;
				//elm_segment_value		    arr_segment_value;


		      int xx= 0;
		  BEGIN

		    logger.info("msisdn: " + JBOSS_ATP_UTIL.vmsisdn + " " + Substr(JBOSS_ATP_UTIL.vmsisdn,1,5));

		    vskip= false;
			      CURSOR c_reff IS
		      
			  			vsql_d="SELECT distinct cust_segmentid FROM atp_par_segmentation WHERE program_id = " + atp_rdm_md.vprogram_id + " ORDER BY cust_segmentid ASC";
					ResultSet x = statement.executeQuery(vsql_d);
					while (x.next()) {
		        elm_segment_id=null;
		        elm_segment_name=null;
		        elm_reff_name=null;
		        elm_reff_column=null;
		        elm_delimiter=null;
		        elm_reff_type=null;
		        elm_prefix_digit=null;
		        elm_valid_return=null;
		        elm_return_no=null;
		        elm_return_multiline=null;
		        elm_segment_value=null;
		        logger.info("x " + x.getString(cust_segmentid));
		        count=0;
					vsql_sg="SELECT a.cust_segmentid, a.segment_name, reff_name, NVL(reff_column,'-') reff_column, NVL(delimiter,'-') delimiter,reff_type, NVL(prefix_digit,0) prefix_digit, valid_return, NVL(return_no,0) return_no, return_multiline, segment_value FROM atp_par_segmentation a, atp_cust_segmentation b WHERE  a.cust_segmentid = b.cust_segmentid AND a.program_id = atp_rdm_md.vprogram_id AND a.cust_segmentid = " + x.getString(cust_segmentid) + " ORDERBY cust_segmentid ASC ";
					ResultSet y = statement.executeQuery(vsql_sg);
					while (y.next()) {
						count=count+1;
						elm_segment_id
						elm_segment_name
						elm_reff_name
						elm_reff_column
						elm_delimiter
						elm_reff_type
						elm_prefix_digit
						elm_valid_return
						elm_return_no
						elm_return_multiline
						elm_segment_value;
					}
		        logger.info("total id " + count);

		        if (elm_reff_type(0) == "TABLE"){
		            logger.info(" ------------");
		            logger.info(" SEGMEN : " + elm_segment_name(0));
		            logger.info(" ------------");
		            if (UPPER(TRIM(elm_segment_name(0))) == "WHITELIST"){
		               vwhite = true; // untuk multiwhitelist

						
		                logger.info("vsql whitelist :" + vsql);
						
						va=null;
						vsql = "SELECT /*+first_rows*/ " + elm_reff_column(0) + " FROM " +  elm_reff_name(0) + " WHERE f_get_progid('" + JBOSS_ATP_UTIL.vmsisdn + "'," + atp_rdm_md.vprogram_id + ") = " + atp_rdm_md.vprogram_id + " AND f_get_evtgroup('" + JBOSS_ATP_UTIL.vmsisdn + "'," + atp_rdm_md.vprogram_id + ") = " + JBOSS_ATP_UTIL.vevt_group_id + " AND " + elm_reff_column(1) + " = '" + JBOSS_ATP_UTIL.vmsisdn + "'";

						ResultSet rs = statement.executeQuery(vsql);
						while (rs.next()) {
							va=rs.getString(elm_reff_column(0));
						}
			
						if (Trim(va)!=null){
							va= 4;
						}

		            }else if (UPPER(TRIM(elm_segment_name(0))) == "NDC PRODUCT"){
		                 logger.info(" jumlah NDC " + count);
						 for (z = 0; z < count; z++) {
		                    
		                     va=null;
		                    
								vsql= "SELECT " + elm_reff_column(z) + " FROM " + elm_reff_name(z) + " WHERE " + elm_delimiter(z) + " = '" + Substr(JBOSS_ATP_UTIL.vmsisdn,1,elm_prefix_digit(z)) + "' AND " +  elm_reff_column(z) " = " + elm_segment_value(z) + " AND cardtype = " + JBOSS_ATP_UTIL.vcardtype;		
								ResultSet rs = statement.executeQuery(vsql);
								while (rs.next()) {
									va=rs.getString(elm_reff_column(z));
								}
								logger.info("ref   : " + elm_reff_column(z));
								logger.info("name  : " + elm_reff_name(z) + "=" + elm_segment_value(z));
								logger.info("delim : " + elm_delimiter(z));

								logger.info(" vsql ndc-" + z + " " + vsql);

								if (va!=null){
									logger.info("FOUND " + elm_segment_name(z) + " " + va + " " + elm_reff_column(z) + "=" + elm_segment_value(z) + " " + JBOSS_ATP_UTIL.vcardtype);
									EXIT;
								}
						 }

		            }else{

		                 if (vskip == false){
		       
		                    logger.info("vsql else :" + vsql);

		                    va = null;
							vsql= "SELECT " + elm_reff_column(0) + " FROM " + elm_reff_name(0) + " WHERE " + elm_delimiter(0) + " = '" + Substr(JBOSS_ATP_UTIL.vmsisdn,1,elm_prefix_digit(1)) + "' ORDER BY " + elm_reff_column(0);
		       
								ResultSet rs = statement.executeQuery(vsql);
								while (rs.next()) {
									va=rs.getString(elm_reff_column(0));
								}

		                    if (va!=null){
		                       logger.info(elm_segment_name(0) + " " + va + " found");
		                       vskip=true;
							}
						 }
					}
			

		            cl=null;
					String cl_segment_value=null;
					vsql="SELECT * FROM atp_par_segmentation WHERE program_id = " + atp_rdm_md.vprogram_id + " AND segment_value = " + va;
					ResultSet rs = statement.executeQuery(vsql);
					while (rs.next()) {
						cl_segment_value=rs.getString(segment_value);
					}

		            logger.info("segment :" + elm_segment_name(0));
		            logger.info("va :" + va);

		            if (cl_segment_value==null){
		                if (upper(elm_segment_name(0)) == "WHITELIST"){
		                    // error raised --
		                    JBOSS_ATP_UTIL.VEVENTMSG = "EVT_NOT_WHITELIST";
						}else if (upper(elm_segment_name(0)) == "NDC PRODUCT"){
		                    //ambil nama product ndc
		                    JBOSS_ATP_UTIL.vnmproduk=null;
							vsql="SELECT DISTINCT product_name FROM ATP_CUST_PREFIX WHERE product_id IN (SELECT segment_value FROM atp_par_segmentation a, atp_cust_segmentation b WHERE  a.cust_segmentid = " + b.cust_segmentid + " AND a.program_id = " + atp_rdm_md.vprogram_id + " AND a.cust_segmentid = 6");
							ResultSet rs = statement.executeQuery(vsql);
							while (rs.next()) {
								JBOSS_ATP_UTIL.vnmproduk = JBOSS_ATP_UTIL.vnmproduk + "," + rs.getString(product_name);
							}

		                    JBOSS_ATP_UTIL.vnmproduk = LTRIM(JBOSS_ATP_UTIL.vnmproduk,',');
		                    

		                   JBOSS_ATP_UTIL.VEVENTMSG  = "EVT_NOT_NDC_EVENT";
						}else if (upper(elm_segment_name(0)) == "HLR REGIONAL"){
		                   JBOSS_ATP_UTIL.VEVENTMSG = "EVT_NOT_IN_REGION";
						}else if (upper(elm_segment_name(0)) == "LOS"){
		                   JBOSS_ATP_UTIL.VEVENTMSG = "EVT_NOT_VALID_LOS";
						}

		               logger.info("NOT FOUND :" + va);
		               JBOSS_ATP_UTIL.VSTATUS= "0";
		               JBOSS_ATP_UTIL.VERRMSG= JBOSS_ATP_UTIL.getmsg (JBOSS_ATP_UTIL.VEVENTMSG);
		               JBOSS_ATP_UTIL.VMSG_OUT= JBOSS_ATP_UTIL.VERRMSG;
		               RAISE_APPLICATION_ERROR (-20010, JBOSS_ATP_UTIL.VERRMSG);
		               EXIT;
		            }

		        }else if (upper(trim(elm_reff_type(0))) == "API"){
		              logger.info("API CEK");

		              vsuccess=null;
		              vretval=null;
		              vresp=null;

		              vdelimiter= REPLACE(elm_delimiter(0),'spasi',' ');
		              vdelimiter= REPLACE(vdelimiter,'SPASI',' ');
		              vdelimiter= REPLACE(vdelimiter,'space',' ');

		              try{
		              vresp= atp_rdm_md.call_api(elm_reff_name(0), JBOSS_ATP_UTIL.vmsisdn, null, null, null);

		              INSERT INTO atp_log_api(log_id, log_msisdn, par_api_name, log_event, log_req,log_retval,log_desc,log_date)
		              VALUES(JBOSS_ATP_UTIL.vtkn_id, JBOSS_ATP_UTIL.vmsisdn, 'SMS ROTS', elm_reff_name(1), JBOSS_ATP_UTIL.vmsisdn, vresp, NULL, SYSDATE);

					  }catch (Exception SQLERRM) {
		                  logger.info("err api : " + sqlerrm);
		                  JBOSS_ATP_UTIL.vstatus= "0";
		                  JBOSS_ATP_UTIL.veventmsg= "EVT_EVENT_ERR_API";
		                  JBOSS_ATP_UTIL.verrmsg= JBOSS_ATP_UTIL.getmsg("EVT_EVENT_ERR_API");
		                  JBOSS_ATP_UTIL.vmsg_out= JBOSS_ATP_UTIL.verrmsg;
		                  raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		                  EXIT;
		              }

		              logger.info("resp :" + vresp);
		              logger.info("valid_return :" + elm_valid_return(0));


		              if (elm_return_multiline(0) == 1){
		                  if (vresp==null || (vresp!=null && UPPER(vresp) NOT LIKE "%" + UPPER(elm_valid_return(1)) + "%")){
		                     JBOSS_ATP_UTIL.vbu_txt= elm_valid_return(0);
		                     JBOSS_ATP_UTIL.vstatus= "0";
		                     JBOSS_ATP_UTIL.veventmsg   = 'EVT_EVENT_NOT_INCOM';
		                     JBOSS_ATP_UTIL.verrmsg        := JBOSS_ATP_UTIL.getmsg('EVT_EVENT_NOT_INCOM');
		                     JBOSS_ATP_UTIL.vmsg_out       := JBOSS_ATP_UTIL.verrmsg;
		                     raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		                     EXIT;
		                  }
		              }else{

		                  vretval = get_delimitedtext(vresp, vdelimiter, to_number(elm_return_no(0)));
		                  //vsuccess  := vretval;--get_delimitedtext(vretval, elm_delimiter(1), elm_return_no(1));
		                  logger.info("SINGLE LINE retval : " + vretval + " " + elm_valid_return(0));
		                  if (vretval==null || (vretval!=null && vretval <> elm_valid_return(0))){
		                     JBOSS_ATP_UTIL.vbu_txt= elm_segment_name(0);
		                     JBOSS_ATP_UTIL.vstatus= "0";
		                     JBOSS_ATP_UTIL.veventmsg="EVT_EVENT_NOT_INCOM";
		                     JBOSS_ATP_UTIL.verrmsg = JBOSS_ATP_UTIL.getmsg("EVT_EVENT_NOT_INCOM");
		                     JBOSS_ATP_UTIL.vmsg_out= JBOSS_ATP_UTIL.verrmsg;
		                     raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		                     EXIT;
		                  }
					}

		              if (vretval!=null){
		                if (vretval <> elm_valid_return(0)){
		                  logger.info("API NOT FOUND :" + vretval);
		                }else{
		                   logger.info("API OK :" + vretval);
		                }
		              }

					}

					}

		  }

		  public void CUST_SEGMENTATION2()throws IOException{

		    /* TODO implementation required */
		    null;
		  }

		  public void call_api(String p_api,String p_msisdn,String p_param2,String p_param3,String p_trxid)throws IOException{



				String REQ;//    UTL_HTTP.REQ;
				String RESP;//   UTL_HTTP.RESP;
				String RETVAL = null;
				String VALUE = null;
				String VEXEC = null;

				VEXEC= null;
				VEXEC= 'http://';

			
				vsql="SELECT * FROM  PAR_API WHERE PAR_API_NAME = " + P_API + " AND PAR_STATUS = 1 ORDER BY PAR_NO ASC";
				ResultSet I = statement.executeQuery(vsql);
				while (I.next()) {
					VEXEC= VEXEC + I.getString(PAR_IN) + I.getString(PAR_VALUE);
			  }


		      VEXEC= REPLACE (VEXEC, '~msisdn',P_MSISDN);
		      VEXEC= REPLACE (VEXEC, '~param2',urlencoder(P_PARAM2));
		      VEXEC= REPLACE (VEXEC, '~param3',urlencoder(P_PARAM3));
		      VEXEC= REPLACE (VEXEC, '~ptrxid',P_TRXID);

		      logger.info(vexec);
		      REQ = UTL_HTTP.BEGIN_REQUEST(VEXEC);
		      RESP= UTL_HTTP.GET_RESPONSE(REQ);

		      UTL_HTTP.READ_LINE(RESP, VALUE, TRUE);
		      logger.info(VALUE);
		      RETVAL := VALUE;
		      UTL_HTTP.END_RESPONSE(RESP);

		      RETURN(RETVAL);

		      EXCEPTION
		      WHEN UTL_HTTP.END_OF_BODY
		      THEN
		        UTL_HTTP.END_RESPONSE(RESP);
		        RETURN(RETVAL);
		      WHEN OTHERS
		      THEN
		      RETURN SQLERRM;
		  }

			public void GET_BLACKLIST()throws IOException{

				cb= null;
				vsql="SELECT * FROM ATP_SUB_BLACKLIST WHERE msisdn = " + JBOSS_ATP_UTIL.vmsisdn + " AND program_id = " + JBOSS_ATP_UTIL.VPROGRAM_ID;
				ResultSet cb = statement.executeQuery(vsql);
				if (cb!=null){
					JBOSS_ATP_UTIL.vstatus = "0";
					JBOSS_ATP_UTIL.veventmsg= "EVT_EVENT_BLACKLIST";
					JBOSS_ATP_UTIL.verrmsg= JBOSS_ATP_UTIL.getmsg("EVT_EVENT_BLACKLIST");
					JBOSS_ATP_UTIL.vmsg_out= JBOSS_ATP_UTIL.verrmsg;
					raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
				}
			}

		  public void GET_WHITELIST(String p_evtid, String p_ref)throws IOException{
		    /* TODO implementation required */
		    return null;
		  }

		  public void logging_sms_mt(String p_msisdn,String p_msg,String p_event)throws IOException{
		    PRAGMA AUTONOMOUS_TRANSACTION;
		  BEGIN
		    INSERT INTO log_sms_mt(MT_MSISDN,MT_EVENT,MT_MESSAGE,MT_DATE,MT_USER)
		    VALUES(p_msisdn, p_event, p_msg, sysdate, 'ATP_RDM_MD');
		    COMMIT;
		    null;
		  }


		   public void get_voucher_bucket()throws IOException{

		   String vrowid=null;
		   String vvoucher=null;
		   String vloc=null;
		   String vtype= "R";
		   String vmsisdn=null;
		    
		    int V_EXP_DATE_EXT;

		  BEGIN
		    cm =null;

			CURSOR c_mailcheck IS
		    SELECT *
		    FROM   atp_parameter_msg_non
		    WHERE  keyword_id = JBOSS_ATP_UTIL.VKEYWORD_ID
		      AND  MSG_TYPE = 'EMAIL';
		    cm     c_mailcheck%ROWTYPE := NULL;
		    OPEN  c_mailcheck;
		    FETCH c_mailcheck INTO cm;
		    CLOSE c_mailcheck;
		    
		    BEGIN
		      SELECT nvl(PAR_VALUE,0) INTO V_EXP_DATE_EXT FROM ATP_PARAMETER WHERE PAR_NAME = 'EXP_VOUCHER_DATE';
		    Exception
		    When Others Then
		      V_EXP_DATE_EXT := 7; //DEFAULT 7 HARI...
		    END;

		    if (Trim(cm.msg_text)!=null){ // notifikasi dikirim via email
		        if (JBOSS_ATP_UTIL.vemail_address==null){
		      		  JBOSS_ATP_UTIL.vstatus = "0";
		            JBOSS_ATP_UTIL.veventmsg = "NO_EMAIL";
		            JBOSS_ATP_UTIL.verrmsg = JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.veventmsg);
		            JBOSS_ATP_UTIL.vmsg_out = JBOSS_ATP_UTIL.verrmsg;
		            raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		        }else{
		            vvoucher=null;
		            vrowid =null;
		            vloc=null;
					    CURSOR c_voucher(xstatus VARCHAR2, xbucket VARCHAR2) is
		    SELECT a.ROWID xrow, a.*,
		    (SELECT product
		    FROM atp_parameter_event
		    WHERE evt_id = a.evt_id
		    AND evt_group_id = a.evt_group_id
		    AND program_id = a.program_id) product
		    FROM atp_voucher_bucket a
		    WHERE a.status = xstatus
		    AND a.evt_id = JBOSS_ATP_UTIL.vevt_id
		    AND a.program_id = ATP_RDM_MD.VPROGRAM_ID
		    FOR UPDATE SKIP LOCKED;
		    cv     c_voucher%ROWTYPE := NULL;
		            FOR x IN c_voucher('A', vtype)
		            LOOP
		               vvoucher := x.voucher_code;
		               vrowid   := x.xrow;
		               vloc     := x.product;
		               EXIT;
		            END LOOP;

		            JBOSS_ATP_UTIL.vbu_txt =null;
					      JBOSS_ATP_UTIL.vbu_txt = vvoucher;

		            if (vvoucher==null){
		              logger.info('not found');
		              --raise error --
		        		  JBOSS_ATP_UTIL.vstatus     := '0';
		              JBOSS_ATP_UTIL.veventmsg   := 'VOUCHER_GIVEOUT';
		              JBOSS_ATP_UTIL.verrmsg     := JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.veventmsg);
		              JBOSS_ATP_UTIL.vmsg_out    := JBOSS_ATP_UTIL.verrmsg;
		              raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		   }else{

		              logger.info('reserved');
		              UPDATE atp_voucher_bucket
		              SET    status        = 'R',
		                     redeemed_by   = JBOSS_ATP_UTIL.vmsisdn,
		                     redeemed_date = SYSDATE,
		                     EXPIRED_DATE = SYSDATE+V_EXP_DATE_EXT,
		                     REDEEM_CHANNEL = JBOSS_ATP_UTIL.vuserid,
		                     BRANCH = PKG_DSP_GW.V_DSP_BRANCH,
		                     REGION = PKG_DSP_GW.V_DSP_REGION,
		                     BRAND = PKG_DSP_GW.V_DSP_BRAND,
		                     LACCI = PKG_DSP_GW.V_DSP_LACCI,
		                     REDEEM_TRX =JBOSS_ATP_UTIL.VTRX_ID
		              WHERE  rowid         = vrowid;

		              JBOSS_ATP_UTIL.vbu_txt := null;
		              JBOSS_ATP_UTIL.vprod1  := null;
		              JBOSS_ATP_UTIL.vbu_txt := vvoucher;
		              JBOSS_ATP_UTIL.vprod1  := vloc;

		              atp_rdm_md.vemail_sender := '"Telkomsel Poin Admin"<telkomsel_poin@telkomsel.co.id>';
		              atp_rdm_md.vemail_subj   := 'Telkomsel Poin - MAP Voucher';

		              atp_rdm_md.send_email;
		   }
		   }

		   }else{  //dikirim via SMS only
		        vvoucher := NULL;
		        vrowid   := NULL;
		        vloc		 := NULL;
		        FOR x IN c_voucher('A', vtype)
		        LOOP
		           vvoucher := x.voucher_code;
		           vrowid   := x.xrow;
		           vloc     := x.product;
		           EXIT;
		        END LOOP;

		        JBOSS_ATP_UTIL.vbu_txt := NULL;
			      JBOSS_ATP_UTIL.vbu_txt := vvoucher;

		        if (vvoucher==null){
		          logger.info("not found");
		          //raise error --
		    		  JBOSS_ATP_UTIL.vstatus = "0";
		          JBOSS_ATP_UTIL.veventmsg= "VOUCHER_GIVEOUT";
		          JBOSS_ATP_UTIL.verrmsg = JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.veventmsg);
		          JBOSS_ATP_UTIL.vmsg_out = JBOSS_ATP_UTIL.verrmsg;
		          raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		        }else{
		          logger.info('reserved');
		          UPDATE atp_voucher_bucket
		          SET    status        = 'R',
		                 redeemed_by   = JBOSS_ATP_UTIL.vmsisdn,
		                 redeemed_date = SYSDATE,
		                 EXPIRED_DATE = SYSDATE+V_EXP_DATE_EXT,
		                     REDEEM_CHANNEL = JBOSS_ATP_UTIL.vuserid,
		                  BRANCH = PKG_DSP_GW.V_DSP_BRANCH,
		                  REGION = PKG_DSP_GW.V_DSP_REGION,
		                  BRAND = PKG_DSP_GW.V_DSP_BRAND,
		                  LACCI = PKG_DSP_GW.V_DSP_LACCI,
		                  REDEEM_TRX =JBOSS_ATP_UTIL.VTRX_ID
		          WHERE  rowid         = vrowid;
		        }
		   }
		   }


		  public void atp_get_voucher()throws IOException{



		      vrowid   VARCHAR2(100) := NULL;
		      vvoucher VARCHAR2(100) := NULL;
		      vtype    CHAR(1) := NULL;

		  BEGIN


		      if (JBOSS_ATP_UTIL.vcardtype =="POSTPAID"){
		         vtype = "P";
		      }else{
		         vtype= "R";
		      }

		       vvoucher= null;
		      vrowid= null;
			        CURSOR c_voucher(xstatus VARCHAR2, xbucket VARCHAR2) is
		      SELECT rowid xrow, a.*
		      FROM   atp_voucher_bucket a
		      WHERE  evt_id = JBOSS_ATP_UTIL.vevt_id
		      AND    status   = xstatus
		      AND    bucket_type = xbucket
		      FOR UPDATE SKIP LOCKED;
		      cv     c_voucher%ROWTYPE := NULL;
		      FOR x IN c_voucher('A', vtype)
		      LOOP
		         vvoucher := x.voucher_code;
		         vrowid   := x.xrow;
		         EXIT;
		      END LOOP;

		      JBOSS_ATP_UTIL.vbu_txt= null;
		      JBOSS_ATP_UTIL.vbu_txt= vvoucher;

		      if (vtype =="P"){
		         if (vvoucher==null){
		            vvoucher= null;
		            vrowid  = null;
		            FOR x IN c_voucher('A', 'R')
		            LOOP
		               vvoucher = x.voucher_code;
		               vrowid = x.xrow;
		               EXIT;
		            END LOOP;

		            JBOSS_ATP_UTIL.vbu_txt= null;
		            JBOSS_ATP_UTIL.vbu_txt= vvoucher;
		         }
		      }

		      logger.info("vcr: " + JBOSS_ATP_UTIL.vbu_txt);
		      logger.info("vrowid: " + vrowid);
		      logger.info("type: " + vtype);

		      if (vvoucher==null){
		        //raise error --
		  		  JBOSS_ATP_UTIL.vstatus= "0";
		        JBOSS_ATP_UTIL.veventmsg= "PEGI2_VCR_GIVEOUT";
		        JBOSS_ATP_UTIL.verrmsg= JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.veventmsg);
		        JBOSS_ATP_UTIL.vmsg_out= JBOSS_ATP_UTIL.verrmsg;
		        raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		      }else{
		        UPDATE atp_voucher_bucket
		        SET    status        = 'R',
		               redeemed_by   = JBOSS_ATP_UTIL.vmsisdn,
		               Redeemed_Date = Sysdate, 
		               REDEEM_CHANNEL = JBOSS_ATP_UTIL.vuserid,
		               CATEGORY_CURRENT = JBOSS_ATP_UTIL.VCATEGORY_CURRENT
		        WHERE  rowid  = vrowid;
		      }

		    Exception
		    When Others Then
		    logger.info("atp_get_voucher :" + sqlerrm);
		         JBOSS_ATP_UTIL.verrmsg= JBOSS_ATP_UTIL.getmsg("REDEEM_EMPTY");
		         JBOSS_ATP_UTIL.veventmsg= "REDEEM_EMPTY";
		         raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		  }


		  public void send_email()throws IOException{

		    String vsender= "Telkomsel Poin Admin"<telkomsel_poin@telkomsel.co.id>"";
		    String vrecipients;
		    String vsubject= "Telkomsel Poin";
		    String vmessage=null;
		    vconn           UTL_SMTP.CONNECTION;

		  BEGIN

		    JBOSS_ATP_UTIL.veventmsg= JBOSS_ATP_UTIL.vkeyword;
		    vsender= atp_rdm_md.vemail_sender;
		    vsubject= atp_rdm_md.vemail_subj;
		    vrecipients= JBOSS_ATP_UTIL.vemail_address;
		    vmessage= JBOSS_ATP_UTIL.getmsg(JBOSS_ATP_UTIL.vkeyword, JBOSS_ATP_UTIL.vkeyword_id, 'EMAIL');

		    vconn= mailer_auth.begin_mail(sender => vsender, recipients => vrecipients, subject => vsubject);
		    mailer_auth.write_text( conn => vconn,message => vmessage);
		    mailer_auth.end_mail(vconn);

		    INSERT INTO atp_email_logs (
		                          log_id,           --1
		                          log_msisdn,       --2
		                          log_email,        --3
		                          log_msg_out,      --4
		                          log_status,       --5
		                          log_date,         --6
		                          log_ipaddrs,      --7
		                          log_event,        --8
		                          log_exec,         --9
		                          log_tkn_id,       --10
		                          program_id,       --11
		                          keyword_id,       --12
		                          evt_set_id,       --13
		                          evt_id_reff)           --14
		                   VALUES(
		                          JBOSS_ATP_UTIL.vtkn_id,   --1
		                          JBOSS_ATP_UTIL.vmsisdn,   --2
		                          JBOSS_ATP_UTIL.vemail_address, --3
		                          vmessage,                --4
		                          '1',                     --5
		                          SYSDATE, 								 --6
		                          NULL,										 --7
		                          JBOSS_ATP_UTIL.veventmsg,      --8
		                          NULL,                    --9
		                          NULL,                    --10
		                          JBOSS_ATP_UTIL.vprogram_id,		 --11
		                          JBOSS_ATP_UTIL.vkeyword_id,    --12
		                          JBOSS_ATP_UTIL.VEVENT_SET_ID,  --13
		                          JBOSS_ATP_UTIL.VEVT_ID);       --14

		  }


		  public void get_multiwhitelist()throws IOException{

		  String vmwhite=null;

		  BEGIN

		    if (vwhite=false){
		        logger.info("unset whitelist");
		        JBOSS_ATP_UTIL.veventmsg= "EVT_INV_WHITELIST";
		        JBOSS_ATP_UTIL.vstatus= "0";
		        JBOSS_ATP_UTIL.verrmsg= JBOSS_ATP_UTIL.getmsg (JBOSS_ATP_UTIL.veventmsg);
		        JBOSS_ATP_UTIL.vmsg_out= JBOSS_ATP_UTIL.verrmsg;
		        raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		    }else{
		  CURSOR c_wh IS
		  SELECT /*+first_rows*/ x.rowid xrow, x.*
		  FROM   ATP_SUB_WHITELIST_ROTS x
		  WHERE  f_get_progid(JBOSS_ATP_UTIL.vmsisdn,atp_rdm_md.vprogram_id)   = atp_rdm_md.vprogram_id
		  AND    f_get_evtgroup(JBOSS_ATP_UTIL.vmsisdn,atp_rdm_md.vprogram_id) = JBOSS_ATP_UTIL.vevt_group_id
		  AND    msisdn       = JBOSS_ATP_UTIL.vmsisdn
		  AND    f_get_msisdncount(JBOSS_ATP_UTIL.vmsisdn,atp_rdm_md.vprogram_id) > 0
		  FOR UPDATE SKIP LOCKED;
		  cw     c_wh%ROWTYPE := NULL;
		        FOR x IN c_wh
		        LOOP
		           vmwhite= null;
		           vxrowid= null;
		           vmwhite= x.msisdn;
		           vxrowid= x.xrow;
		           EXIT;
		        END LOOP;

		        logger.info("msisdn whitelist : " + vmwhite);
		        logger.info("msisdn rowid : " + vxrowid);

		        if (vmwhite==null){
		            logger.info("msisdn not found");
		            JBOSS_ATP_UTIL.veventmsg= "WHITELIST_RUNOUT";
		            JBOSS_ATP_UTIL.vstatus  = "0";
		            JBOSS_ATP_UTIL.verrmsg  = JBOSS_ATP_UTIL.getmsg (JBOSS_ATP_UTIL.veventmsg);
		            JBOSS_ATP_UTIL.vmsg_out = JBOSS_ATP_UTIL.verrmsg;
		            raise_application_error (-20010, JBOSS_ATP_UTIL.verrmsg);
		        }

		    }

		  }

			public void flag_multiwhitelist()throws IOException{
				logger.info("start update " + vxrowid);
				PROC_INSERT_WHITELIST_RD(JBOSS_ATP_UTIL.vmsisdn, atp_rdm_md.vprogram_id, JBOSS_ATP_UTIL.vevt_group_id,'WHITELIST','API','WHITELIST',JBOSS_ATP_UTIL.VEVT_ID,'1', null, null, null, null );

				JBOSS_ATP_UTIL.vtgl_txt= TO_CHAR(SYSDATE,'DD/MM/YY');
			}
}
