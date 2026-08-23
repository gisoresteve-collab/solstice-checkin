import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


# ============================================================
# SOLSTICE EVENTS CO.
# ASYNCHRONOUS CHECK-IN SERVICE
# ============================================================

# ------------------------------------------------------------
# ATTENDEE DATA
# ------------------------------------------------------------

attendees = {
    "A001": {
        "name": "LINAH",
        "status": "NOT_CHECKED_IN",
        "job_id": None
    },

    "A002": {
        "name": "STEVE",
        "status": "NOT_CHECKED_IN",
        "job_id": None
    },

    "A003": {
        "name": "PHENNY",
        "status": "NOT_CHECKED_IN",
        "job_id": None
    }
}


# ------------------------------------------------------------
# ASYNCHRONOUS MESSAGE QUEUE
# ------------------------------------------------------------

print_queue = asyncio.Queue()

# Protects the critical scan operation.
state_lock = asyncio.Lock()


# ------------------------------------------------------------
# REQUEST MODELS
# ------------------------------------------------------------

class ScanRequest(BaseModel):
    attendee_id: str


class WebhookRequest(BaseModel):
    job_id: str
    attendee_id: str
    status: str


# ------------------------------------------------------------
# FASTAPI APPLICATION
# ------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):

    # Start three simulated printer workers.
    workers = [
        asyncio.create_task(printer_worker(1)),
        asyncio.create_task(printer_worker(2)),
        asyncio.create_task(printer_worker(3))
    ]

    print("Solstice asynchronous printer workers started.")

    yield

    # Stop workers when application shuts down.
    for worker in workers:
        worker.cancel()

    await asyncio.gather(
        *workers,
        return_exceptions=True
    )

    print("Printer workers stopped.")


app = FastAPI(
    title="Solstice Events Co. Check-In Service",
    description="Asynchronous badge printing prototype for the Meridian Pivot",
    version="1.0.0",
    lifespan=lifespan
)


# ------------------------------------------------------------
# HOME
# ------------------------------------------------------------

@app.get("/")
async def home():

    return {
        "service": "Solstice Events Co. Check-In Service",
        "status": "running",
        "architecture": "asynchronous queue + webhook"
    }


# ------------------------------------------------------------
# VIEW ALL ATTENDEES
# ------------------------------------------------------------

@app.get("/attendees")
async def get_attendees():

    return {
        "attendees": attendees
    }


# ------------------------------------------------------------
# VIEW ONE ATTENDEE
# ------------------------------------------------------------

@app.get("/attendees/{attendee_id}")
async def get_attendee(attendee_id: str):

    attendee_id = attendee_id.upper()

    if attendee_id not in attendees:
        raise HTTPException(
            status_code=404,
            detail="Attendee not found"
        )

    return {
        "attendee_id": attendee_id,
        **attendees[attendee_id]
    }


# ------------------------------------------------------------
# SCAN ATTENDEE
# ------------------------------------------------------------

@app.post("/scan")
async def scan_attendee(request: ScanRequest):

    attendee_id = request.attendee_id.upper()

    # Make sure attendee exists.
    if attendee_id not in attendees:
        raise HTTPException(
            status_code=404,
            detail="Attendee not found"
        )

    # Lock the scan operation so two simultaneous scans
    # cannot create two print jobs.
    async with state_lock:

        attendee = attendees[attendee_id]

        # ----------------------------------------------------
        # DUPLICATE SCAN PROTECTION
        # ----------------------------------------------------

        if attendee["status"] in {
            "PENDING",
            "PRINTING",
            "CHECKED_IN"
        }:

            return {
                "attendee_id": attendee_id,
                "name": attendee["name"],
                "status": attendee["status"],
                "message": (
                    "Duplicate scan rejected. "
                    "No second badge will be printed."
                )
            }

        # ----------------------------------------------------
        # CREATE UNIQUE PRINT JOB
        # ----------------------------------------------------

        job_id = f"JOB-{uuid4().hex[:8].upper()}"

        # Attendee is NOT checked in yet.
        attendee["status"] = "PENDING"

        # Associate the job with the attendee.
        attendee["job_id"] = job_id

        # ----------------------------------------------------
        # PUBLISH REQUEST TO MESSAGE QUEUE
        # ----------------------------------------------------

        await print_queue.put({
            "job_id": job_id,
            "attendee_id": attendee_id
        })

    print(
        f"[KIOSK] {attendee_id} scanned. "
        f"{job_id} added to print queue."
    )

    # IMPORTANT:
    # We return immediately.
    # We do NOT wait for the printer.
    return {
        "attendee_id": attendee_id,
        "name": attendee["name"],
        "job_id": job_id,
        "status": "PENDING",
        "message": (
            "Print request queued. "
            "Awaiting printer confirmation."
        )
    }


# ------------------------------------------------------------
# PRINTER WORKER
# ------------------------------------------------------------

async def printer_worker(worker_number: int):

    while True:

        job = await print_queue.get()

        try:

            job_id = job["job_id"]
            attendee_id = job["attendee_id"]

            print(
                f"[PRINTER WORKER {worker_number}] "
                f"Processing {job_id} for {attendee_id}"
            )

            # Different processing times deliberately demonstrate
            # that confirmations can arrive out of order.
            delays = {
                "A001": 3,
                "A002": 1,
                "A003": 2
            }

            delay = delays.get(attendee_id, 2)

            await asyncio.sleep(delay)

            print(
                f"[PRINTER WORKER {worker_number}] "
                f"{job_id} completed printing."
            )

            # Simulate the vendor calling our webhook.
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://solstice"
            ) as client:

                response = await client.post(
                    "/webhook/printer",
                    json={
                        "job_id": job_id,
                        "attendee_id": attendee_id,
                        "status": "PRINTED"
                    }
                )

                print(
                    f"[PRINTER WORKER {worker_number}] "
                    f"Webhook response: {response.status_code}"
                )

        except Exception as error:

            print(
                f"[PRINTER WORKER {worker_number}] "
                f"Error: {error}"
            )

        finally:

            print_queue.task_done()


# ------------------------------------------------------------
# PRINTER WEBHOOK
# ------------------------------------------------------------

@app.post("/webhook/printer")
async def printer_webhook(request: WebhookRequest):

    job_id = request.job_id
    attendee_id = request.attendee_id

    # Verify attendee.
    if attendee_id not in attendees:
        raise HTTPException(
            status_code=404,
            detail="Attendee not found"
        )

    async with state_lock:

        attendee = attendees[attendee_id]

        # ----------------------------------------------------
        # JOB OWNERSHIP VALIDATION
        # ----------------------------------------------------

        if attendee["job_id"] != job_id:

            raise HTTPException(
                status_code=400,
                detail="Job does not belong to attendee"
            )

        # ----------------------------------------------------
        # SUCCESSFUL PRINT
        # ----------------------------------------------------

        if request.status == "PRINTED":

            # Webhook idempotency.
            # If the same callback arrives twice,
            # don't check the attendee in twice.
            if attendee["status"] == "CHECKED_IN":

                return {
                    "attendee_id": attendee_id,
                    "job_id": job_id,
                    "status": "CHECKED_IN",
                    "message": "Webhook already processed."
                }

            # ONLY NOW do we mark the attendee as checked in.
            attendee["status"] = "CHECKED_IN"

            print(
                f"[WEBHOOK] {job_id} confirmed."
            )

            print(
                f"[CHECK-IN] {attendee_id} is now CHECKED_IN."
            )

            return {
                "attendee_id": attendee_id,
                "job_id": job_id,
                "status": "CHECKED_IN",
                "message": "Badge printed successfully."
            }

    raise HTTPException(
        status_code=400,
        detail="Unsupported printer status"
    )


# ------------------------------------------------------------
# RUNNING APPLICATION
# ------------------------------------------------------------

if __name__ == "__main__":

    import uvicorn

    uvicorn.run(
        "main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )