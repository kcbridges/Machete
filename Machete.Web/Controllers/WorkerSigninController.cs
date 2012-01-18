﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using Machete.Service;
using Machete.Helpers;
using Machete.Web.ViewModel;
using Machete.Web.Helpers;
using NLog;
using Machete.Domain;
using Machete.Data;
using Microsoft.Reporting.WebForms;
using Machete.Web.Models;
using System.Data.Objects.SqlClient;
using System.Data.Objects;
using System.Web.Routing;

namespace Machete.Web.Controllers
{
   [ElmahHandleError]
    public class WorkerSigninController : Controller
    {
        private readonly IWorkerSigninService _serv;
        private readonly IWorkerService _wServ;
        private Logger log = LogManager.GetCurrentClassLogger();
        private System.Globalization.CultureInfo CI;
        private LogEventInfo levent = new LogEventInfo(LogLevel.Debug, "WorkerSigninController", "");
        public WorkerSigninController(IWorkerSigninService workerSigninService, IWorkerService workerService)
        {
            this._serv = workerSigninService;
            this._wServ = workerService;
        }

        protected override void Initialize(RequestContext requestContext)
        {
            base.Initialize(requestContext);
            CI = (System.Globalization.CultureInfo)Session["Culture"];
        }
        //
        // GET: /WorkerSignin/
        // Initial page creation
        [Authorize(Roles = "Manager, Administrator, Check-in")]
        public ActionResult Index()
        {        
            WorkerSigninViewModel _model = new WorkerSigninViewModel();
            if (ViewBag.dateforsignin == null) _model.dateforsignin = DateTime.Today;
            else _model.dateforsignin = ViewBag.dateforsignin;
            _model.workersignins = _serv.getView(_model.dateforsignin);
            _model.last_chkin_image = new Image();
            return View(_model);
        }
        //
        // subsequent page creations
        [HttpPost]
        [Authorize(Roles = "Manager, Administrator, Check-in")]
        public ActionResult Index(int dwccardentry, DateTime dateforsignin)
        {
            // Signin the card just swiped
            var _signin = new WorkerSignin();
            _signin.dwccardnum = dwccardentry;
            _signin.dateforsignin = dateforsignin;
            //
            //
            _serv.CreateWorkerSignin(_signin, this.User.Identity.Name);
            
            var _model = new WorkerSigninViewModel();
            var worker = _wServ.GetWorkerByNum(dwccardentry);
            //Get expiration date from checkin, show with next view
            _model.last_chkin_memberexpirationdate = _serv.getExpireDate(dwccardentry);         
            if (_model.last_chkin_memberexpirationdate < DateTime.Now) 
            {
                _model.memberexpired = true;
            } else {
                _model.memberexpired = false;
            }
            //Get picture from checkin, show with next view
            var checkin_image = _serv.getImage(dwccardentry);           
            string imageRef = "/Image/GetImage/0";
            if (checkin_image != null)
            {
                _model.last_chkin_image = checkin_image;
                imageRef = "/Image/GetImage/" + checkin_image.ID;
            }
            else
            {
                _model.last_chkin_image = new Image();

            }
            //_model.dateforsignin = dateforsignin;                       //Pass date back for date checkin continuity
            //ModelState.Remove("dwccardentry");                          // Clears previous entry from view for next iteration
            //_model.workersignins = _serv.getView(_model.dateforsignin); //Get list of signins already checked in for the day
            //return View(_model);

            return Json(new
            {                
                memberExpired = _model.memberexpired,
                memberInactive = worker.memberStatus == LookupCache.getSingleEN("memberstatus", "Inactive") ? true : false,
                memberSanctioned = worker.memberStatus == LookupCache.getSingleEN("memberstatus", "Sanctioned") ? true : false,
                memberExpelled = worker.memberStatus == LookupCache.getSingleEN("memberstatus", "Expelled") ? true : false,
                imageRef = imageRef,
                expirationDate = _model.last_chkin_memberexpirationdate
            },
            JsonRequestBehavior.AllowGet);
        }

        //
        // Lottery post
        //
        // POST: /WorkOrder/Delete/5
        [HttpPost, UserNameFilter]
        [Authorize(Roles = "Administrator, Manager, PhoneDesk")]
        public ActionResult lotterySignin(int lotterycardnum, DateTime lotterysignindate, string user)
        {
            //var workOrder = woServ.GetWorkOrder(id);
            string rtnstatus;
            var wsi = _serv.GetWorkerSignin(lotterycardnum, lotterysignindate);
            if (wsi != null)
            {
                if (wsi.lottery_timestamp == null)
                {
                    wsi.lottery_timestamp = DateTime.Now;
                    _serv.SaveWorkerSignin();
                }                
                rtnstatus = "OK";

            }
            else
            {
                rtnstatus = "NO_WSI_REC";
            }
            var _signin = new WorkerSignin();
            return Json(new
            {
                status = rtnstatus,
                memberID = lotterycardnum
            },
            JsonRequestBehavior.AllowGet);
        }
        //
        // GET: /WorkerSignin/Delete/5
        [Authorize(Roles = "Administrator, Manager, Check-in")]
        public ActionResult Delete(int id)
        {
            string status = null;
            try
            {
                _serv.DeleteWorkerSignin(id);
            }
            catch (Exception e)
            {
                status = RootException.Get(e, "WorkerSigninService");
            }

            return Json(new
            {
                status = status ?? "OK",
                deletedID = id
            },
            JsonRequestBehavior.AllowGet);
        }


        [Authorize(Roles = "Administrator, Manager, Check-in")]
        public ActionResult AjaxHandler(jQueryDataTableParam param)
        {
            ServiceIndexView<WorkerSigninView> was = _serv.GetIndexView(new DispatchOptions {
                CI = CI,
                search = param.sSearch,
                date = param.todaysdate == null ? null : (DateTime?)DateTime.Parse(param.todaysdate),
                dwccardnum = Convert.ToInt32(param.dwccardnum),
                woid = Convert.ToInt32(param.searchColName("WOID")),
                orderDescending = param.sSortDir_0 == "asc" ? false : true,
                displayStart = param.iDisplayStart,
                displayLength = param.iDisplayLength,
                sortColName = param.sortColName(),
                wa_grouping = param.wa_grouping,
                typeofwork_grouping = param.typeofwork_grouping
            });

            //return what's left to datatables
            var result = from p in was.query
                         select new {  WSIID = p.ID,
                                       recordid = p.ID.ToString(),
                                       dwccardnum = p.dwccardnum,
                                       fullname = p.fullname,
                                       firstname1 = p.firstname1,
                                       firstname2 = p.firstname2,
                                       lastname1 = p.lastname1,
                                       lastname2 = p.lastname2, 
                                       dateforsignin = p.dateforsignin,
                                       dateforsigninstring = p.dateforsignin.ToShortDateString(),
                                       WAID = p.waid ?? 0,
                                       memberStatus = LookupCache.byID(p.memberStatus, CI.TwoLetterISOLanguageName),
                                       memberInactive = p.memberStatus == LookupCache.getSingleEN("memberstatus", "Inactive") ? true : false,
                                       memberSanctioned = p.memberStatus == LookupCache.getSingleEN("memberstatus", "Sanctioned") ? true : false,
                                       memberExpired = p.expirationDate < p.dateforsignin ? true : false,
                                       memberExpelled = p.memberStatus == LookupCache.getSingleEN("memberstatus", "Expelled") ? true : false,
                                       imageID = p.imageID,
                                       expirationDate = p.expirationDate.ToShortDateString(),
                                       skills = _getSkillCodes(p.englishlevel, p.skill1, p.skill2, p.skill3)
                         };
            return Json(new
            {
                sEcho = param.sEcho,
                iTotalRecords = was.totalCount,
                iTotalDisplayRecords = was.filteredCount,
                aaData = result
            },
            JsonRequestBehavior.AllowGet);
        }
        private string _getSkillCodes(int eng, int? sk1, int? sk2, int? sk3)
        {
            string rtnstr = "E" + eng + " ";
            if (sk1 !=null) {
                var lookup = LookupCache.getBySkillID((int)sk1);
                rtnstr = rtnstr + lookup.ltrCode + lookup.level + " ";
            }
            if (sk2 != null)
            {
                var lookup = LookupCache.getBySkillID((int)sk2);
                rtnstr = rtnstr + lookup.ltrCode + lookup.level + " ";
            }
            if (sk3 != null)
            {
                var lookup = LookupCache.getBySkillID((int)sk3);
                rtnstr = rtnstr + lookup.ltrCode + lookup.level;
            }
            return rtnstr;
        }
    }
}
