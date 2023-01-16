const APP_ID = '0cda0df5ba1b407399139c40763e1a71'
const CHANNEL = 'main'
const TOKEN = '007eJxTYLgkomUc2XopW23zMoGH6QzCOUly39W/flzSzifIaLZ7EocCg0FySqJBSpppUqJhkomBubGlpaGxZTKQZWacaphobsj+5EhyQyAjw3afaFZGBggE8VkYchMz8xgYAHq8HJU='
let UID;
const client = AgoraRTC.createClient({mode:'rtc',codec:'h264'})

let localTracks = []
let remoteUsers = {}

let joinAndDisplayLocalStream = async () =>{
    console.log(client)
    client.on('user-published', onUserJoined    ())
    UID = await client.join(APP_ID,CHANNEL,TOKEN,null)
    localTracks = await AgoraRTC.createMicrophoneAndCameraTracks()

    let player = `<div class="video-container" id="user-container-${UID}">
                    <div class="username-wrapper"><span class="user-name">My Name</span></div>
                    <div class="video-player"id="user-${UID}" ></div>
                </div>`
    document.getElementById('video-streams').insertAdjacentHTML('beforeend',player)

    localTracks[1].play(`user-${UID}`)

    await client.publish(localTracks[0],localTracks[1])
}

let onUserJoined = async (user, mediaType) => {
    remoteUsers[user.uid] = user
    await client.subscribe(user,mediaType)

    if (mediaType === 'video'){
        let player = document.getElementById(`user-container-${user.uid}`)
        if (player != null){
            player.remove()
        }
        player = `<div class="video-container" id="user-container-${user.uid}">
                    <div class="username-wrapper"><span class="user-name">My Name</span></div>
                    <div class="video-player"id="user-${user.uid}" ></div>
                </div>`
        document.getElementById('video-streams').insertAdjacentHTML('beforeend',player)
        user.videoTrack.play(`user-${user.uid}`)
    }

    if (mediaType === 'audio'){
        user.audioTrack.play()
    }
}
joinAndDisplayLocalStream()
