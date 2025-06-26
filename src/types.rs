use serde::{self, Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct MojangResponse {
    pub id: String,
    pub name: String,
    #[serde(rename = "profileActions")]
    pub profile_actions: Vec<ProfileAction>,
    pub properties: Vec<Property>,
}

#[derive(Serialize, Deserialize)]
pub struct ProfileAction {
    pub action: String,
}

#[derive(Serialize, Deserialize)]
pub struct Property {
    pub name: String,
    pub value: String,
    pub signature: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct Textures {
    pub timestamp: i64,
    #[serde(rename = "profileId")]
    pub profile_id: String,
    #[serde(rename = "profileName")]
    pub profile_name: String,
    #[serde(rename = "signatureRequired")]
    pub signature_required: Option<bool>,
    pub textures: TexturesClass,
}

#[derive(Serialize, Deserialize)]
pub struct TexturesClass {
    #[serde(rename = "SKIN")]
    pub skin: Option<Skin>,
    #[serde(rename = "CAPE")]
    pub cape: Option<Cape>,
}

#[derive(Serialize, Deserialize)]
pub struct Cape {
    pub url: String,
}

#[derive(Serialize, Deserialize)]
pub struct Skin {
    pub url: String,
    pub metadata: Option<Metadata>,
}

#[derive(Serialize, Deserialize)]
pub struct Metadata {
    pub model: String,
}
