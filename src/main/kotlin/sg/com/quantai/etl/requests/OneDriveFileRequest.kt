package sg.com.quantai.etl.requests

import java.time.LocalDateTime

data class OneDriveFileRequest(
    val name: String,
    val lastModified: LocalDateTime,
    val downloadUrl: String
)